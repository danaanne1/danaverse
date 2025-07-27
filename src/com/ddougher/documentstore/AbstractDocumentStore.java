package com.ddougher.documentstore;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractDocumentStore implements DocumentStore {
	
	protected final Supplier<Document> docFromNothing;
	protected final Function<ByteBuffer, Document> docFromBytes;
	protected final Supplier<String> idSupplier;
	
	
	public AbstractDocumentStore(Optional<Supplier<Document>> docFromNothing, Optional<Function<ByteBuffer, Document>> docFromBytes,
                                 Optional<Supplier<String>> idSupplier) {
		super();
		this.docFromNothing = docFromNothing.orElse(BuffDocument::new);
		this.docFromBytes = docFromBytes.orElse((bytes)->new BuffDocument(bytes));
		this.idSupplier = idSupplier.orElse(()-> TimeBasedUUIDGenerator.instance().nextUUID().toString());
	}

	public AbstractDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes,
                                 Supplier<String> idSupplier) {
		this(Optional.of(docFromNothing), Optional.of(docFromBytes), Optional.of(idSupplier));
	}

	public AbstractDocumentStore(Supplier<Document> docFromNothing, Function<ByteBuffer, Document> docFromBytes) {
		this(Optional.of(docFromNothing), Optional.of(docFromBytes),Optional.empty());
	}

	public AbstractDocumentStore() {
		this(Optional.empty(), Optional.empty(), Optional.empty());
	}

	@Override
	public Document newInstance() {
		return newInstance(idSupplier.get());
	}

	public final Document withDocStore(Document document) {
		return withDocStore(document,this);
	}
	
	public static final Document withDocStore(Document document, DocumentStore store) {
		if (document instanceof DocumentStoreAware) {
			((DocumentStoreAware)document).setDocumentStore(store);
		}
		return document;
	}

}
