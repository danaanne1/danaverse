package com.ddougher.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import com.ddougher.util.BigArray.Addressable;
import com.ddougher.util.BigArray.AssetFactory;
import com.ddougher.util.BigArray.Sizeable;

/*
 * Must support serialization via a recreation strategy
 * Must support embedded size and reference counting
 * Must ignore tid's
 * 
 * This is WORM strategy ONLY.
 * 
 * block = physicalOffset / Integer.MAX_INT
 * file_number = block/40
 * 
 * the first (intsize) bytes of an addressable indicate the size
 * the next (intsize) bytes of an addressable indicate the reference count
 * 
 * if the reference count is zero, then the addressable is garbage collectible
 * 
 * garbage collection should be parasitic, and move the low watermark one block per operation
 * 
 * if the low watermark moves past a block boundary, then a file can be removed.
 * 
 */
public class NonTrackingSharedMemoryAssetFactory implements AssetFactory {

	final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	final Map<Integer, RandomAccessFile> files = new HashMap<Integer, RandomAccessFile>();
	final Map<Integer, MappedByteBuffer> blocks = new HashMap<Integer, MappedByteBuffer>();

	File baseName;
	AtomicLong highWatermark;
	AtomicLong lowWatermark;
	
	class NonTrackingSharedMemoryAddressable implements Addressable {
		long physicalOffset = 0L;
		int size = 0;
		
		@Override
		public void set(ByteBuffer data, UUID tid) {
			releaseBlockAt(physicalOffset);
			size = data.limit();
			physicalOffset = writeBlock(new ByteBuffer [] { data });
		}

		@Override
		public void set(Addressable src, UUID tid) {
			releaseBlockAt(physicalOffset);
			physicalOffset = ((NonTrackingSharedMemoryAddressable)src).physicalOffset;
			size = ((NonTrackingSharedMemoryAddressable)src).size;
			acquireBlockAt(physicalOffset);
		}

		@Override
		public void append(Addressable a, UUID tid) {
			long [] oldOffsets = { physicalOffset, ((NonTrackingSharedMemoryAddressable)a).physicalOffset };
			ByteBuffer [] buffers = {
					retrieveBlockAt(physicalOffset), 
					retrieveBlockAt(((NonTrackingSharedMemoryAddressable)a).physicalOffset) 
			};
			physicalOffset = writeBlock(buffers);
			size = buffers[0].limit() + buffers[1].limit();
			releaseBlockAt(oldOffsets[0]);
			releaseBlockAt(oldOffsets[1]);
		}

		@Override
		public int size(UUID tid) {
			return size;
		}

		@Override
		public ByteBuffer get(UUID tid) {
			return retrieveBlockAt(physicalOffset);
		}
		
	}
	
	
	@Override
	public Addressable createAddressable() {
		return new NonTrackingSharedMemoryAddressable();
	}

	@Override
	public Sizeable createSizeable() {
		// TODO Auto-generated method stub
		return null;
	}

}
