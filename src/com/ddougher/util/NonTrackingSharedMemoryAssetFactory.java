package com.ddougher.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.ShortBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

import com.ddougher.util.BigArray.Addressable;
import com.ddougher.util.BigArray.AssetFactory;
import com.ddougher.util.BigArray.Sizeable;

/*
 * Must support serialization via a recreation strategy
 * Must support embedded size and reference counting
 * Must ignore tid's
 * 
 * This is WORM strategy ONLY. (except for reference counts)
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

	final int BLOCK_MAX = Integer.MAX_VALUE;
	final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	final Map<Integer, RandomAccessFile> files = new HashMap<Integer, RandomAccessFile>();
	final Map<Integer, MappedByteBuffer> blocks = new HashMap<Integer, MappedByteBuffer>();
	
	File baseName;
	AtomicLong highWatermark = new AtomicLong(1L);
	AtomicLong lowWatermark;
	
	
	private ByteBuffer assertBlock(int blockNumber) {
		// TODO Auto-generated method stub
		return null;
	}

	public long writeSlice(ByteBuffer[] byteBuffers) {
		// decide if the current block can hold the data
		// if not, write a pre-freed slice
		
		// maybe allocate a new block

		// write the new record, and dont forget to raise the highWatermark
		
		return 0;
	}

	public ByteBuffer retrieveSliceAt(long physicalOffset) {
		int blockNumber = (int)(physicalOffset==0?0:physicalOffset/BLOCK_MAX);
		int offsetInBlock = (int) (physicalOffset%BLOCK_MAX);
		ByteBuffer block = assertBlock(blockNumber);
		ByteBuffer retVal;
		int size;
		synchronized(block) {
			size = block.getInt(offsetInBlock); 
			block.position(offsetInBlock+8);
			retVal = block.slice();
		}
		retVal.limit(size);
		return retVal.asReadOnlyBuffer();
	}

	public void acquireSliceAt(long physicalOffset) {
		int blockNumber = (int)(physicalOffset==0?0:physicalOffset/BLOCK_MAX);
		int offsetInBlock = (int) (physicalOffset%BLOCK_MAX);
		ByteBuffer block = assertBlock(blockNumber);
		synchronized (block) {
			block.putInt(offsetInBlock+4, block.getInt(offsetInBlock+4)+1 );
		}
	}

	public void releaseSliceAt(long physicalOffset) {
		int blockNumber = (int)(physicalOffset==0?0:physicalOffset/BLOCK_MAX);
		int offsetInBlock = (int) (physicalOffset%BLOCK_MAX);
		ByteBuffer block = assertBlock(blockNumber);
		int refCount;
		int size;
		synchronized (block) {
			refCount = block.getInt(offsetInBlock+4)-1;
			block.putInt(offsetInBlock+4, refCount);
		}
		if (refCount <= 0) 
			raiseLowWaterMark(physicalOffset);
	}

	private void raiseLowWaterMark(long physicalOffset) {
		// successively read blocks, as long as their refCounts are <= 0, raise the lwm
		// if you lwm past a file boundary, delete the old file
	}


	class NonTrackingSharedMemoryAddressable implements Addressable {
		long physicalOffset = 0L;
		int size = 0;
		
		
		@Override
		public void set(ByteBuffer data, UUID tid) {
			releaseSliceAt(physicalOffset);
			size = data.limit();
			physicalOffset = writeSlice(new ByteBuffer [] { data });
		}

		@Override
		public void set(Addressable src, UUID tid) {
			releaseSliceAt(physicalOffset);
			physicalOffset = ((NonTrackingSharedMemoryAddressable)src).physicalOffset;
			size = ((NonTrackingSharedMemoryAddressable)src).size;
			acquireSliceAt(physicalOffset);
		}

		@Override
		public void append(Addressable a, UUID tid) {
			long [] oldOffsets = { physicalOffset, ((NonTrackingSharedMemoryAddressable)a).physicalOffset };
			ByteBuffer [] buffers = {
					retrieveSliceAt(physicalOffset), 
					retrieveSliceAt(((NonTrackingSharedMemoryAddressable)a).physicalOffset) 
			};
			physicalOffset = writeSlice(buffers);
			size = buffers[0].limit() + buffers[1].limit();
			releaseSliceAt(oldOffsets[0]);
			releaseSliceAt(oldOffsets[1]);
		}

		@Override
		public int size(UUID tid) {
			return size;
		}

		@Override
		public ByteBuffer get(UUID tid) {
			return retrieveSliceAt(physicalOffset);
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
