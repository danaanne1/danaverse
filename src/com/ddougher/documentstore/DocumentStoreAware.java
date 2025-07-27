package com.ddougher.documentstore;

public interface DocumentStoreAware {
	
	public void setDocumentStore(DocumentStore docStore);
	public DocumentStore getDocumentStore();

}
