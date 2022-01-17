package com.ddougher.util;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import com.ddougher.util.BigArray.Addressable;
import com.ddougher.util.BigArray.AssetFactory;
import com.ddougher.util.BigArray.Sizeable;

public class MemoryAssetFactory implements AssetFactory {

	@Override
	public Addressable createAddressable() {
		return new MemAddressable();
	}

	
	@Override
	public Sizeable createSizeable() {
		return new MemSizeable();
	}
	
	private class MemSizeable implements Sizeable {
		TreeMap<UUID,BigInteger> sizes = new TreeMap<>(BigArray.timePrioritizedComparator);
		
		public MemSizeable() {
			super();
		}

		private MemSizeable(TreeMap<UUID, BigInteger> sizes) {
			super();
			this.sizes = sizes;
		}

		@Override
		public void set(BigInteger size, UUID mark) {
			sizes.put(mark, size);
		}
		
		@Override
		public BigInteger get(UUID mark) {
			Map.Entry<UUID, BigInteger> me = sizes.floorEntry(mark);
			if (me==null)
				return BigInteger.ZERO;
			return me.getValue();
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public Sizeable duplicate() {
			return new MemSizeable((TreeMap<UUID, BigInteger>)sizes.clone());
		}
	}

	private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	
	private class MemAddressable implements Addressable {
		TreeMap<UUID,ByteBuffer> memory = new TreeMap<UUID, ByteBuffer>(BigArray.timePrioritizedComparator);

		@Override
		public int size(UUID mark) {
			return get(mark).limit();
		}
		
		@Override
		public void set(Addressable src, UUID mark) {
			memory.put(mark, src.get(mark));
		}
		
		@Override
		public void set(ByteBuffer data, UUID mark) {
			memory.put(mark, data);
		}
		
		@Override
		public ByteBuffer get(UUID mark) {
			Map.Entry<UUID, ByteBuffer> buf = memory.floorEntry(mark);
			if (buf == null)
				return EMPTY_BUFFER;
			return buf.getValue();
		}
		
		@Override
		public void append(Addressable a, UUID mark) {
			
			ByteBuffer buf = get(mark);
			ByteBuffer buf2 = a.get(mark);
			
			ByteBuffer newBuffer = ByteBuffer.allocate(buf.limit()+buf2.limit());
			newBuffer.put((ByteBuffer) buf.duplicate().rewind());
			newBuffer.put((ByteBuffer) buf2.duplicate().rewind());
			
			// replace the old byte arrays with references to the combined one
			Map.Entry<UUID, ByteBuffer> entry = memory.floorEntry(mark);
			if (entry != null) 
				// replace with the copied value so the old can be GC
				memory.put(entry.getKey(), (ByteBuffer) newBuffer.duplicate().limit(buf.limit()));
			
			if (a instanceof MemAddressable) {
				entry = ((MemAddressable)a).memory.floorEntry(mark);
				if (entry != null) 
					// replace with the copied value so the old can be GC
					((MemAddressable)a).memory.put(entry.getKey(), ((ByteBuffer) newBuffer.duplicate().position(buf.limit())).slice());
			}
			
			set(newBuffer,mark);
			
		}
		public String toString() {
			ByteBuffer b = get(null);
			byte [] buf = new byte[Math.min(20, b.limit())];
			((ByteBuffer) b.duplicate().rewind()).get(buf);
			return Arrays.toString(buf);
		}
		
	};
	
}
