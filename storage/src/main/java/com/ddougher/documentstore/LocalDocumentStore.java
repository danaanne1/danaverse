package com.ddougher.documentstore;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An in memory document store suitable for use within the same VM.
 * 
 * @author Dana
 */
public class LocalDocumentStore extends AbstractDocumentStore implements DocumentStore {

	private static class Record {
		String documentId;
		ByteBuffer document;
		long versionNumber;
		long lockedUntil;
		String lockId;
		public Record(String documentId, ByteBuffer document, long versionNumber, String lockId) {
			super();
			this.documentId = documentId;
			this.document = document;
			this.versionNumber = versionNumber;
			this.lockId = lockId;
		}
		public Record(String documentId, ByteBuffer document, long versionNumber) {
			this(documentId, document, versionNumber, null);
		}
	}

	Map<Document,Record> recordsByDocument = Collections.synchronizedMap(new WeakHashMap<>());
	ConcurrentHashMap<String,Record> recordsById = new ConcurrentHashMap<String,Record>();
	
	public LocalDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes, Supplier<String> idSupplier) {
		super(docFromNothing, docFromBytes, idSupplier);
	}

	public LocalDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes) {
		super(docFromNothing, docFromBytes);
	}

	public LocalDocumentStore() {
		super();
	}
	
	@Override
	public String getID(Document document) {
		return documentsRecord(document).documentId;
	}

	@Override
	public Document newInstance(String key) {
		Document doc = docFromNothing.get();
		recordsByDocument.put(doc, new Record(key,null,0));
		return withDocStore(doc);
	}

	@Override
	public Document get(String key) {
		Record storageRecord = recordsById.get(key);
		if (storageRecord == null)
			return newInstance(key);

		Document doc = docFromBytes.apply(storageRecord.document);
		recordsByDocument.put(doc, new Record(storageRecord.documentId,null,storageRecord.versionNumber));	
		return withDocStore(doc);
	}

	@Override
	public void put(Document document) {
		Record documentsRecord = documentsRecord(document);
		
		synchronized(documentsRecord.documentId) {
			Record storageRecord = recordsById.get(documentsRecord.documentId);

			assertVersionHolder(documentsRecord, storageRecord);

			assertLockHolder(documentsRecord, storageRecord);
			
			// always put a new storage record, which also resets locks:
			recordsById.put( documentsRecord.documentId, storageRecord = new Record(documentsRecord.documentId, document.toByteBuffer(), documentsRecord.versionNumber+1 ) );

			// update the document record:
			recordsByDocument.put(document, new Record(storageRecord.documentId, null, storageRecord.versionNumber));
		}
	}

	@Override
	public void delete(Document document) {
		Record documentsRecord =  documentsRecord(document);
		synchronized(documentsRecord.documentId) {
			Record storageRecord = recordsById.get(documentsRecord.documentId);

			assertVersionHolder(documentsRecord, storageRecord);

			assertLockHolder(documentsRecord, storageRecord);

			// no need to release, since the current record no longer has a lock
			recordsById.remove(documentsRecord.documentId);
		}
	}
	
	@Override
	public Document lock(String key) {
		Record storageRecord = recordsById.get(key);
		synchronized ( storageRecord==null?key:storageRecord.documentId ) {
			// just in case, always get the latest copy after we enter the sync block:
			storageRecord = recordsById.get(key); 

			assertLockHolder(null, storageRecord);

			// insert a new item if required:
			if (storageRecord == null) { 
				storageRecord = new Record(key,docFromNothing.get().toByteBuffer(),0);
				recordsById.put(key, storageRecord);
			}

			// set the lock
			storageRecord.lockedUntil = System.currentTimeMillis()+60_000;
			storageRecord.lockId = idSupplier.get().toString();	
		}
		
		// Once the lock is established go about standard retrieval
		Document doc = docFromBytes.apply(storageRecord.document);

		// modify the record for the lock holding document to indicate this is the lock holder:
		recordsByDocument.put(doc, new Record(storageRecord.documentId, null, storageRecord.versionNumber, storageRecord.lockId ));

		return withDocStore(doc);

	}
	
	@Override
	public void release(Document document) {
		Record documentsRecord = documentsRecord(document);
		synchronized (documentsRecord.documentId) {
			Record storageRecord = recordsById.get(documentsRecord.documentId); // just in case, always get the latest copy

			assertLockHolder(documentsRecord, storageRecord);

			storageRecord.lockedUntil = 0;
		}
	}

	private void assertLockHolder(Record documentsRecord, Record storageRecord) {
		if ( 
				( storageRecord != null )
				&& ( storageRecord.lockedUntil > System.currentTimeMillis() )
				&& ( 
						( documentsRecord == null )
						|| ( storageRecord.lockId != documentsRecord.lockId ) // SIC: equivalence not equals plz
				) 
		) {
			throw new ConcurrentModificationException("Not the lock holder.");
		}
	}

	private void assertVersionHolder(Record documentsRecord, Record storageRecord) {
		if ( ( storageRecord == null ) && ( documentsRecord.versionNumber != 0) )
			throw new ConcurrentModificationException("Version mismatch. Use a new instance.");
		if ( ( storageRecord != null ) && ( documentsRecord.versionNumber!=storageRecord.versionNumber)) 
			throw new ConcurrentModificationException("Version mismatch.");
	}

	private Record documentsRecord(Document document) {
		Record documentsRecord = recordsByDocument.get(document);
		if (documentsRecord == null)
			throw new IllegalArgumentException("Unknown document");
		return documentsRecord;
	}
	
}
