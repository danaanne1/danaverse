package com.ddougher.market;

import java.util.function.Consumer;
import java.util.function.Function;

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
			throw new IllegalArgumentException("document is not an DocumentStoreAware");
		DocumentStore ds = ((DocumentStoreAware)document).getDocumentStore();
		if (!ObservableDocumentStore.class.isAssignableFrom(ds.getClass()))
			throw new IllegalArgumentException("documents DocumentStore is not an ObservableDocumentStore");
		return f.apply((ObservableDocumentStore)ds);
	}

	
	
}
