package com.ddougher.proxamic;

import javax.print.Doc;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * A place to store and retrieve documents.
 */
@SuppressWarnings("RedundantOperationOnEmptyContainer")
public interface DocumentStore {

	/**
	 * Returns the id for a document.
	 * 
	 * @param document
	 * @return
	 */
	public String getID(Document document);

	/**
	 * Creates a new instance of a document with a random id
	 * @return
	 */
	public Document newInstance();

	/**
	 * Creates a new instance of a document with a known id.
	 * Note that if a put is attempted and the key already exists this will likely result in an error.
	 * @param key
	 * @return 
	 */
	public Document newInstance(String key);

	/**
	 * Returns the document for the provided key, or a new instance (as if by calling newInstance(key)) if no document presently exists with that key
	 * 
	 * @param key
	 * @return
	 */
	public Document get(String key);
	
	/**
	 * Behaves exactly like get, except that the returned instance holds the documents lock. This method can fail if another instance holds the lock.
	 * @param key
	 * @return
	 */
	public Document lock(String key);
	
	public void release(Document document);

	public void put(Document document);

	public void delete(Document document);

	/**
	 *  Returns all the keys between startKey and endKey. If startKey == null, then starts from the beginning. Likewise if endKey == null ends at the end.
	 */
	public default Stream<String> traverseKeys(String startKey, String endKey) { List<String> s = Collections.emptyList(); return s.stream(); }

	public default Stream<Document> traverseDocuments(String startKey, String endKey) { return traverseKeys(startKey,endKey).map(this::get); }


	// Syntactic sugar methods:

	public default <T extends DocumentView> String getID(T documentView) { return getID(documentView.document()); }
	
	public default <T extends DocumentView> T newInstance(Class<T> viewClass) { return newInstance().as(viewClass); }

	public default <T extends DocumentView> T newInstance(Class<T> viewClass, String key) { return newInstance(key).as(viewClass); }

	public default <T extends DocumentView> T get(Class<T> viewClass, String key) { return get(key).as(viewClass); }

	public default <T extends DocumentView> T lock(Class<T> viewClass, String key) { return lock(key).as(viewClass); }

	public default <T extends DocumentView> void release(T documentView) { release(documentView.document()); }

	public default <T extends DocumentView> void put(T documentView) { put(documentView.document()); }

	public default <T extends DocumentView> void delete(T documentView) { delete(documentView.document()); }

	/** 
	 * Runs an execution as a transaction. The transaction will either succeed or throw an exception.
	 * @param transaction
	 */
	public default void transact(Consumer<DocumentStore> transactor) { new TransactingDocumentStore(this).accept(transactor); }

	/** 
	 * Runs a scoped execution similar to a transaction but without transactional semantics. This is useful for queries that require 
	 * help with cannonicalization.
	 * @param transaction
	 */
	public default void execute(Consumer<DocumentStore> executor) { new CachingDocumentStore(this).accept(executor); }
	
}
