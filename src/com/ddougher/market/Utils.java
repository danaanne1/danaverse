package com.ddougher.market;

import java.awt.event.ActionEvent;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.swing.AbstractAction;
import javax.swing.Action;

import com.ddougher.proxamic.ObservableDocumentStore;
import com.theunknowablebits.proxamic.Document;
import com.theunknowablebits.proxamic.DocumentStore;
import com.theunknowablebits.proxamic.DocumentStoreAware;

public final class Utils {

	public static void withObservableDocStoreFromDocument(Document document, Consumer<ObservableDocumentStore> c) {
		withObservableDocStoreFromDocument(document, ds->{
			c.accept(ds);
			return null;
		});
	}

	public static <T> T withObservableDocStoreFromDocument(Document document, Function<ObservableDocumentStore, T> f) {
		if (!DocumentStoreAware.class.isAssignableFrom(document.getClass()))
			throw new IllegalArgumentException("document is not DocumentStoreAware");
		DocumentStore ds = ((DocumentStoreAware)document).getDocumentStore();
		if (!ObservableDocumentStore.class.isAssignableFrom(ds.getClass()))
			throw new IllegalArgumentException("DocumentStore is not Observable");
		return f.apply((ObservableDocumentStore)ds);
	}

	@SuppressWarnings("serial")
	public static Action actionFu(String label, Runnable runnable) { 
		return new AbstractAction(label) { @Override public void actionPerformed(ActionEvent e) {  runnable.run(); }	};
	}

}
