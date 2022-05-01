package com.ddougher.util;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

public interface AssetFactory {

	public interface Addressable extends Serializable {
		void set(ByteBuffer data);
		void set(Addressable src);
		void append(Addressable a);
		AssetFactory factory();
		int size();
		ByteBuffer get();

		/** swaps a and b */
		default void swap(Addressable b, UUID tid) {
			ByteBuffer tmp = b.get();
			b.set(get());
			set(tmp);
		}
		
		/** changes the effective size of this addressable. Essentially the same as set(get(mark).duplicate().position(length).flip(),mark) */
		default void resize(int length, UUID tid) {
			set((ByteBuffer)get().duplicate().position(length).flip());
		}
	}

	Addressable createAddressable();

	default Addressable createAddressable(ByteBuffer data) {
		Addressable a = createAddressable();
		a.set(data);
		return a;
	}

}