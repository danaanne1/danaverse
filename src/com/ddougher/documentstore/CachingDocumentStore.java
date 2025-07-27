package com.ddougher.documentstore;

import java.util.HashMap;
import java.util.function.Consumer;

final class CachingDocumentStore implements DocumentStore, Consumer<Consumer<DocumentStore>> {
	DocumentStore delegate;

	HashMap<String, Document> documentsById = new HashMap<>();
	
	public CachingDocumentStore(DocumentStore delegate) {
		this.delegate = delegate;
	}
	
	
	@Override
	public String getID(Document document) { return delegate.getID(document); }

	@Override
	public Document newInstance() { return AbstractDocumentStore.withDocStore(delegate.newInstance(),this); }

	@Override
	public Document newInstance(String key) { return AbstractDocumentStore.withDocStore(delegate.newInstance(key),this); }

	@Override
	public Document get(String key) {
		if (!documentsById.containsKey(key))
			documentsById.put(key, AbstractDocumentStore.withDocStore(delegate.lock(key), this));
		return documentsById.get(key);
	}

	@Override
	public synchronized void put(Document document) {
		delegate.put(document);
		documentsById.put(getID(document), AbstractDocumentStore.withDocStore(document, this));
	}

	@Override
	public synchronized void delete(Document document) {
		delegate.delete(document);
		documentsById.remove(getID(document));
	}

	@Override
	public void transact(Consumer<DocumentStore> transaction) {
		TransactingDocumentStore transactor = new TransactingDocumentStore(this);
		boolean success = false;
		try {
			transaction.accept(transactor);
			success = true;
		} finally {
			if (success)
				transactor.commit();
			else
				transactor.rollback();
		}
	}

	@Override
	public void execute(Consumer<DocumentStore> execution) {
		execution.accept(this);
	}
	@Override
	public synchronized Document lock(String key) {
		if (!documentsById.containsKey(key))
			documentsById.put(key, AbstractDocumentStore.withDocStore(delegate.lock(key),this));
		return documentsById.get(key);
	}
	@Override
	public void release(Document document) {
		delegate.release(document);
	}


	@Override
	public void accept(Consumer<DocumentStore> executor) {
		executor.accept(this);
		
	}
}