package com.ddougher.proxamic;

import java.util.EventListener;
import java.util.EventObject;

import com.theunknowablebits.proxamic.Document;
import com.theunknowablebits.proxamic.DocumentStore;

public interface DocumentListener extends EventListener {

	@SuppressWarnings("serial")
	class DocumentEvent extends EventObject {
		String docId;
		public DocumentEvent(DocumentStore source, String docId) {
			super(source);
			this.docId = docId;
		}
		public DocumentStore docStore() {
			return (DocumentStore)super.getSource();
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
