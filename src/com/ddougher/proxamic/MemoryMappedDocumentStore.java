package com.ddougher.proxamic;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;
import java.util.function.Supplier;

import com.ddougher.util.AssetFactory;
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
public class MemoryMappedDocumentStore extends AbstractDocumentStore implements DocumentStore {

	AssetFactory assetFactory;
	ConcurrentNavigableMap<String, Record> index;
	
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

	
	public MemoryMappedDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes, Supplier<String> idSupplier) {
		super(docFromNothing, docFromBytes, idSupplier);
		assetFactory = new MemoryMappedAssetFactory();
		index = new ConcurrentSkipListMap<String, Record>();
	}

	public MemoryMappedDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes) {
		super(docFromNothing, docFromBytes);
		assetFactory = new MemoryMappedAssetFactory();
		index = new ConcurrentSkipListMap<String, Record>();
	}

	public MemoryMappedDocumentStore() {
		super();
		assetFactory = new MemoryMappedAssetFactory();
		index = new ConcurrentSkipListMap<String, Record>();
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
		}
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
