package com.ddougher.documentstore;

import java.nio.ByteBuffer;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An in memory document store suitable for use across multiple VM's.
 * 
 * @author Dana
 */
public class MemoryDocumentStore extends AbstractDocumentStore implements DocumentStore {

	private class Record {
		ByteBuffer document;
		long versionNumber;
		long lockedUntil;
		String lockId;
		public Record(ByteBuffer document, long versionNumber, String lockId) {
			super();
			this.document = document;
			this.versionNumber = versionNumber;
			this.lockId = lockId;
		}
		public Record(ByteBuffer document, long versionNumber) {
			this(document, versionNumber, null);
		}
		public String toString() {
			StringBuffer buffer = new StringBuffer(String.format("(Lock:%s, LockedUntil:%d, Version:%d)", lockId, lockedUntil, versionNumber));
			buffer.append(docFromBytes.apply(document).toString());
			return buffer.toString();
		}
	}

	ConcurrentHashMap<String,Record> recordsById = new ConcurrentHashMap<String,Record>();
	
	
	public void dump() {
		for (Map.Entry<String,Record> me:recordsById.entrySet() ) {
			System.out.println("========================" + me.getKey() + "========================" );
			System.out.println(me.getValue().toString());
		}
	}
	
	public MemoryDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes, Supplier<String> idSupplier) {
		super(docFromNothing, docFromBytes, idSupplier);
	}

	public MemoryDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes) {
		super(docFromNothing, docFromBytes);
	}

	public MemoryDocumentStore() {
		super();
	}
	
	interface MemoryDocument extends DocumentView {
		@Getter("__ID__") String ID();
		@Setter("__ID__") MemoryDocument withID(String value);
		@Getter("__LOCK__") String LOCK();
		@Setter("__LOCK__") MemoryDocument withLOCK(String value);
		@Getter("__VERSION__") Long VERSION();
		@Setter("__VERSION__") MemoryDocument withVERSION(Long value);
	}
	
	@Override
	public String getID(Document document) {
		String result = document.as(MemoryDocument.class).ID();
		if (result==null) throw new IllegalArgumentException();
		return result;
	}

	@Override
	public Document newInstance(String key) {
		Document doc = docFromNothing.get();
		doc.as(MemoryDocument.class).withID(key).withVERSION(0L);
		return withDocStore(doc);
	}

	@Override
	public Document get(String key) {
		Record storageRecord = recordsById.get(key);
		if (storageRecord == null)
			return newInstance(key);

		Document doc = docFromBytes.apply(storageRecord.document);
		doc
			.as(MemoryDocument.class)
			.withID(key)
			.withVERSION(storageRecord.versionNumber);
		
		return withDocStore(doc);
	}

	@Override
	public void put(Document document) {
		String docId = document.as(MemoryDocument.class).ID().intern();
		synchronized(docId) {
			Record storageRecord = recordsById.get(docId);
			if (storageRecord == null)
				storageRecord = new Record(null, 0);

			assertVersionHolder(document, storageRecord);

			assertLockHolder(document, storageRecord);
			
			// always put a new storage record, which also resets locks:
			recordsById.put( docId, storageRecord = new Record(document.toByteBuffer(), storageRecord.versionNumber+1 ) );
			
			document.as(MemoryDocument.class).withVERSION(storageRecord.versionNumber);
		}
	}

	@Override
	public void delete(Document document) {
		String docId = document.as(MemoryDocument.class).ID().intern();
		synchronized(docId) {
			Record storageRecord = recordsById.get(docId);

			assertVersionHolder(document, storageRecord);

			assertLockHolder(document, storageRecord);

			// no need to release, since the current record no longer has a lock
			recordsById.remove(docId);
		}
	}
	
	@Override
	public Document lock(String key) {
		String docId = key.intern();
		Record storageRecord;
		synchronized ( docId ) {
			// just in case, always get the latest copy after we enter the sync block:
			storageRecord = recordsById.get(key); 

			assertLockHolder(null, storageRecord);

			// insert a new item if required:
			if (storageRecord == null) { 
				storageRecord = new Record(docFromNothing.get().toByteBuffer(),0);
				recordsById.put(docId, storageRecord);
			}

			// set the lock
			storageRecord.lockedUntil = System.currentTimeMillis()+60_000;
			storageRecord.lockId = idSupplier.get().toString();	
		}
		
		// Once the lock is established go about standard retrieval
		Document doc = docFromBytes.apply(storageRecord.document);

		// modify the record for the lock holding document to indicate this is the lock holder:
		doc
			.as(MemoryDocument.class)
			.withID(docId)
			.withLOCK(storageRecord.lockId)
			.withVERSION(storageRecord.versionNumber);

		return withDocStore(doc);

	}
	
	@Override
	public void release(Document document) {
		String docId = document.as(MemoryDocument.class).ID().intern();
		synchronized (docId) {
			Record storageRecord = recordsById.get(docId); // just in case, always get the latest copy

			assertLockHolder(document, storageRecord);

			storageRecord.lockedUntil = 0;
		}
	}

	private void assertLockHolder(Document document, Record storageRecord) {
		if ( 
				( storageRecord != null )
				&& ( storageRecord.lockedUntil > System.currentTimeMillis() )
				&& ( document == null || storageRecord.lockId != document.as(MemoryDocument.class).LOCK() ) 
		) {
			throw new ConcurrentModificationException("Not the lock holder.");
		}
	}

	private void assertVersionHolder(Document document, Record storageRecord) {
		if ( ( storageRecord == null ) && ( document.as(MemoryDocument.class).VERSION() != 0) )
			throw new ConcurrentModificationException("Version mismatch. Use a new instance.");
		if ( ( storageRecord != null ) && ( document.as(MemoryDocument.class).VERSION()!=storageRecord.versionNumber)) 
			throw new ConcurrentModificationException("Version mismatch.");
	}

}
