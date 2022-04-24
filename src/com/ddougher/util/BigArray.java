package com.ddougher.util;

import java.io.Serializable;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.function.Function;

public interface BigArray {

	public static final Comparator<UUID> timePrioritizedComparator = new Comparator<UUID>() {
		@Override
		public int compare(UUID o1, UUID o2) {
			if (o1 == o2)
				return 0;
			if (o1 == null) 
				return 1;
			if (o2 == null)
				return -1;
			return Long.compare(o1.timestamp(), o2.timestamp());
		}
	};

	public interface AssetFactory {
		Addressable createAddressable();
		Sizeable createSizeable();
		default Addressable createAddressable(ByteBuffer data, UUID tid) {
			Addressable a = createAddressable();
			a.set(data, tid);
			return a;
		}
		default Sizeable createSizeable(BigInteger size, UUID tid) {
			Sizeable s = createSizeable();
			s.set(size, tid);
			return s;
		}
	}

	public interface Sizeable {
		void set(BigInteger value, UUID tid);
		BigInteger get(UUID tid);
		Sizeable duplicate();
		// converts the incoming sizeable to a series of transactional deltas and merges it with a duplicate of this one, returning the newly merged sizeable
		Sizeable merge(Sizeable incoming);
		SortedSet<UUID> marks();
	}
	
	public interface Addressable extends Serializable {
		void set(ByteBuffer data, UUID tid);
		void set(Addressable src, UUID tid);
		void append(Addressable a, UUID tid);
		AssetFactory factory();
		int size(UUID tid);
		ByteBuffer get(UUID tid);

		/** swaps a and b */
		default void swap(Addressable b, UUID tid) {
			ByteBuffer tmp = b.get(tid);
			b.set(get(tid), tid);
			set(tmp,tid);
		}
		
		/** changes the effective size of this addressable. Essentially the same as set(get(mark).duplicate().position(length).flip(),mark) */
		default void resize(int length, UUID tid) {
			set((ByteBuffer)get(tid).duplicate().position(length).flip(),tid);
		}
	}

	String dump();

	/**
	 * Inserts a block of data at the given offset.
	 * 
	 * @param data
	 * @param length
	 */
	void insert(ByteBuffer data, BigInteger offset);

	BigInteger size();

	void remove(BigInteger offset, BigInteger length);

	Iterator<ByteBuffer> get(BigInteger offset, BigInteger length);

	<V> V transact(Function<BigArray, V> transaction);

	void transact(Consumer<BigArray> transaction);

	BigInteger size(UUID tid);

	Iterator<ByteBuffer> get(BigInteger offset, BigInteger length, UUID tid);


}