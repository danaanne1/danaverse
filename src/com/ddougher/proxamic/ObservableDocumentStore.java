package com.ddougher.proxamic;

import java.util.EventListener;
import java.util.EventObject;

import com.theunknowablebits.proxamic.Document;
import com.theunknowablebits.proxamic.DocumentStore;

public interface ObservableDocumentStore extends DocumentStore {

	public interface Listener extends EventListener {

		@SuppressWarnings("serial")
		class DocumentEvent extends EventObject {
			String docId;
			public DocumentEvent(ObservableDocumentStore source, String docId) {
				super(source);
				this.docId = docId;
			}
			public ObservableDocumentStore docStore() {
				return (ObservableDocumentStore)super.getSource();
			}
			public String docId() {
				return docId;
			}
			public Document document() {
				return docStore().get(docId);
			}
			
		}
		
		void documentPut(DocumentEvent event);

		void documentDeleted(DocumentEvent event);
	}

	void addListener(String docId, Listener listener);

	void removeListener(String docId, Listener listener);

}