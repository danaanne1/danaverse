package com.ddougher.documentstore;

import java.util.*;
import java.util.function.Consumer;

final class TransactingDocumentStore implements DocumentStore, Consumer<Consumer<DocumentStore>> {
	Set<Document> membership = Collections.synchronizedSet(new LinkedHashSet<>());
	DocumentStore delegate;

	HashMap<String, Document> documentsById = new HashMap<>();
	LinkedHashMap<String, Document> toPut = new LinkedHashMap<>();
	LinkedHashMap<String, Document> toDelete = new LinkedHashMap<>();
	
	public TransactingDocumentStore(DocumentStore delegate) {
		this.delegate = delegate;
	}
	
	private Document adopt(Document document) {
		membership.add(document);
		return document;
	}
	
	private Document checkMembership(Document document) {

		if (!membership.contains(document))
			throw new IllegalArgumentException("document is not a member of this transaction or a containing transaction");
		return document;
	}
	
	@Override
	public String getID(Document document) { return delegate.getID(checkMembership(document)); }

	@Override
	public Document newInstance() { return adopt(AbstractDocumentStore.withDocStore(delegate.newInstance(),this)); }

	@Override
	public Document newInstance(String key) { return adopt(AbstractDocumentStore.withDocStore(delegate.newInstance(key),this)); }

	@Override
	public Document get(String key) {
		return lock(key);
	}

	@Override
	public synchronized void put(Document document) {
		checkMembership(document);
		toPut.put(getID(document), document);
	}

	@Override
	public synchronized void delete(Document document) {
		checkMembership(document);

		// add to the delete list
		toDelete.put(getID(document), document);

		// remove any puts, this allows a put after delete 
		toPut.remove(getID(document), document);
	}

	@Override
	public void transact(Consumer<DocumentStore> transaction) {
		transaction.accept(this);
	}

	@Override
	public void execute(Consumer<DocumentStore> execution) {
		execution.accept(this);
	}
	@Override
	public synchronized Document lock(String key) {
		if (toPut.containsKey(key)) 
			return toPut.get(key);
		if (toDelete.containsKey(key))
			return newInstance(key);
		if (!documentsById.containsKey(key))
			documentsById.put(key, AbstractDocumentStore.withDocStore(delegate.lock(key),this));
		return adopt(documentsById.get(key));
	}
	
	@Override
	public void release(Document document) {
		// noop
	}
	
	final void commit() {
		documentsById.keySet().removeAll(toDelete.keySet());
		documentsById.keySet().removeAll(toPut.keySet());

		toDelete.values().parallelStream().forEach(document->delegate.delete(document));

		toPut.values().parallelStream().forEach(document->delegate.put(document));

		documentsById.values().parallelStream().forEach(document->delegate.release(document));
	}
	final void rollback() {
		documentsById.values().parallelStream().forEach(document->delegate.release(document));
	}

	@Override
	public void accept(Consumer<DocumentStore> transactor) {
		boolean success = false;
		try {
			transactor.accept(this);
			success = true;
		} finally {
			if (success)
				commit();
			else
				rollback();
		}
	}
}