package com.ddougher.proxamic;

import java.io.Serializable;
import java.nio.ByteBuffer;

/** All document classes implement this */
public interface Document extends Serializable {

	public <T extends DocumentView> T as(final Class<T> documentClass);

	public Document newInstance();

	public default <T extends DocumentView> T newInstance(Class<T> viewClass) {
		return newInstance().as(viewClass);
	}

	public void remove(String fieldName);
	
	ByteBuffer toByteBuffer();
	
	byte [] toBytes();
	
}
