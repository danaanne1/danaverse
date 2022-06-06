package com.ddougher.proxamic;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ddougher.proxamic.ObservableDocumentStore.Listener.DocumentEvent;
import com.ddougher.util.AssetFactory.Addressable;
import com.ddougher.util.MemoryMappedAssetFactory;
import com.theunknowablebits.proxamic.AbstractDocumentStore;
import com.theunknowablebits.proxamic.Document;
import com.theunknowablebits.proxamic.DocumentStore;
import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Setter;

/**
 * A memory mapped document store
 * 
 * @author Dana
 */
public class MemoryMappedDocumentStore extends AbstractDocumentStore implements DocumentStore, Serializable, Closeable, ObservableDocumentStore {

	private static final long serialVersionUID = 1L;
	MemoryMappedAssetFactory assetFactory;
	ConcurrentNavigableMap<String, Record> index;
	transient Map<String, Set<Listener>> observers;
	
	
	private class Record implements Serializable {
		private static final long serialVersionUID = 1L;
		Addressable addressable;
		long versionNumber;
		long lockedUntil;
		String lockId;
		public Record(Addressable document, long versionNumber, String lockId) {
			super();
			this.addressable = document;
			this.versionNumber = versionNumber;
			this.lockId = lockId;
		}
		public Record(Addressable document, long versionNumber) {
			this(document, versionNumber, null);
		}
		public String toString() {
			StringBuffer buffer = new StringBuffer(String.format("(Lock:%s, LockedUntil:%d, Version:%d)", lockId, lockedUntil, versionNumber));
			buffer.append(docFromBytes.apply(addressable.get()).toString());
			return buffer.toString();
		}
	}

	public MemoryMappedDocumentStore
	(
			Optional<String> basePath, 
			Optional<Integer> blockSize,
			Optional<Supplier<Document>> docFromNothing, 
			Optional<Function<ByteBuffer, Document>> docFromBytes, 
			Optional<Supplier<String>> idSupplier
	) {
		super(docFromNothing, docFromBytes, idSupplier);
		assetFactory = new MemoryMappedAssetFactory(basePath, blockSize);
		index = new ConcurrentSkipListMap<String, Record>();
		observers = Collections.synchronizedMap(new WeakHashMap<String, Set<Listener>>());
	}

	public MemoryMappedDocumentStore() {
		super();
		assetFactory = new MemoryMappedAssetFactory();
		index = new ConcurrentSkipListMap<String, Record>();
		observers = Collections.synchronizedMap(new WeakHashMap<String, Set<Listener>>());
	}
	
	private Map<String, Set<Listener>> assertObserversExists() {
		synchronized(this) {
			if (observers == null) 
				observers = Collections.synchronizedMap(new WeakHashMap<String, Set<Listener>>());
		}
		return observers;
	}
	
	@Override
	public void addListener(String docId, Listener listener) {
		synchronized(assertObserversExists()) {
			if (!observers.containsKey(docId))
				observers.put(docId, new CopyOnWriteArraySet<Listener>());
			observers.get(docId).add(listener);
		}
	}

	@Override
	public void removeListener(String docId, Listener listener) {
		synchronized(assertObserversExists()) {
			if (!observers.containsKey(docId))
				observers.put(docId, new CopyOnWriteArraySet<Listener>());
			observers.get(docId).remove(listener);
		}
	}

	@Override
	public void close() throws IOException {
		assetFactory.close();
	}
	
	interface MemoryMappedDocument extends DocumentView {
		@Getter("__ID__") String ID();
		@Setter("__ID__") MemoryMappedDocument withID(String value);
		@Getter("__LOCK__") String LOCK();
		@Setter("__LOCK__") MemoryMappedDocument withLOCK(String value);
		@Getter("__VERSION__") Long VERSION();
		@Setter("__VERSION__") MemoryMappedDocument withVERSION(Long value);
	}
	
	@Override
	public String getID(Document document) {
		String result = document.as(MemoryMappedDocument.class).ID();
		if (result==null) throw new IllegalArgumentException();
		return result;
	}

	@Override
	public Document newInstance(String key) {
		Document doc = docFromNothing.get();
		doc.as(MemoryMappedDocument.class).withID(key).withVERSION(0L);
		return withDocStore(doc);
	}

	@Override
	public Document get(String key) {
		Record storageRecord = index.get(key);
		if (storageRecord == null)
			return newInstance(key);

		Document doc = docFromBytes.apply(storageRecord.addressable.get());
		doc
			.as(MemoryMappedDocument.class)
			.withID(key)
			.withVERSION(storageRecord.versionNumber);
		
		return withDocStore(doc);
	}

	@Override
	public void put(Document document) {
		String docId = document.as(MemoryMappedDocument.class).ID().intern();
		synchronized(docId) {
			Record storageRecord = index.get(docId);
			if (storageRecord == null)
				storageRecord = new Record(assetFactory.createAddressable(), 0);

			assertVersionHolder(document, storageRecord);

			assertLockHolder(document, storageRecord);
			
			// always put a new storage record, which also resets locks:
			index.put( docId, storageRecord = new Record(storageRecord.addressable, storageRecord.versionNumber+1 ) );
			storageRecord.addressable.set(document.toByteBuffer());
			
			document.as(MemoryMappedDocument.class).withVERSION(storageRecord.versionNumber);
		}
		DocumentEvent evt = new DocumentEvent(this, docId);
		assertObserversExists()
			.getOrDefault(docId, Collections.emptySet())
			.parallelStream()
			.forEach(o -> o.documentPut(evt));
	}

	@Override
	public void delete(Document document) {
		String docId = document.as(MemoryMappedDocument.class).ID().intern();
		synchronized(docId) {
			Record storageRecord = index.get(docId);

			assertVersionHolder(document, storageRecord);

			assertLockHolder(document, storageRecord);

			// no need to release, since the current record no longer has a lock
			index.remove(docId);
			storageRecord.addressable.free();
		}
		DocumentEvent evt = new DocumentEvent(this, docId);
		assertObserversExists()
			.getOrDefault(docId, Collections.emptySet())
			.parallelStream()
			.forEach(o -> o.documentDeleted(evt));
	}
	
	@Override
	public Document lock(String key) {
		String docId = key.intern();
		Record storageRecord;
		synchronized ( docId ) {
			// just in case, always get the latest copy after we enter the sync block:
			storageRecord = index.get(key); 

			assertLockHolder(null, storageRecord);

			// insert a new item if required:
			if (storageRecord == null) { 
				storageRecord = new Record(assetFactory.createAddressable(docFromNothing.get().toByteBuffer()),0);
				index.put(docId, storageRecord);
			}

			// set the lock
			storageRecord.lockedUntil = System.currentTimeMillis()+60_000;
			storageRecord.lockId = idSupplier.get().toString();	
		}
		
		// Once the lock is established go about standard retrieval
		Document doc = docFromBytes.apply(storageRecord.addressable.get());

		// modify the record for the lock holding document to indicate this is the lock holder:
		doc
			.as(MemoryMappedDocument.class)
			.withID(docId)
			.withLOCK(storageRecord.lockId)
			.withVERSION(storageRecord.versionNumber);

		return withDocStore(doc);

	}
	
	@Override
	public void release(Document document) {
		String docId = document.as(MemoryMappedDocument.class).ID().intern();
		synchronized (docId) {
			Record storageRecord = index.get(docId); // just in case, always get the latest copy

			assertLockHolder(document, storageRecord);

			storageRecord.lockedUntil = 0;
		}
	}

	private void assertLockHolder(Document document, Record storageRecord) {
		if ( 
				( storageRecord != null )
				&& ( storageRecord.lockedUntil > System.currentTimeMillis() )
				&& ( document == null || storageRecord.lockId != document.as(MemoryMappedDocument.class).LOCK() ) 
		) {
			throw new ConcurrentModificationException("Not the lock holder.");
		}
	}

	private void assertVersionHolder(Document document, Record storageRecord) {
		if ( ( storageRecord == null ) && ( document.as(MemoryMappedDocument.class).VERSION() != 0) )
			throw new ConcurrentModificationException("Version mismatch. Use a new instance.");
		if ( ( storageRecord != null ) && ( document.as(MemoryMappedDocument.class).VERSION()!=storageRecord.versionNumber)) 
			throw new ConcurrentModificationException("Version mismatch.");
	}


}
