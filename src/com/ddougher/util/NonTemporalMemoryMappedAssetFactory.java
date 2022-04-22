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
 * if the reference count is < 0, then the addressable is garbage collectible
 * if the reference count is 0, this is an incomplete writeSlice (callers should acquire after write)
 * 		this makes it easy to find and garbage collect incomplete slices at restart
 * 
 * garbage collection should be parasitic, and move the low watermark quickly
 * 
 * if the low watermark moves past a block boundary, then a file can be removed.
 * 
 */
public class NonTemporalMemoryMappedAssetFactory implements AssetFactory {

	final int BLOCK_MAX = Integer.MAX_VALUE;
	final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	final Map<Integer, RandomAccessFile> files = new HashMap<Integer, RandomAccessFile>();
	final Map<Integer, MappedByteBuffer> blocks = new HashMap<Integer, MappedByteBuffer>();
	
	File baseName = new File("data");
	AtomicLong highWatermark = new AtomicLong(1L);
	AtomicLong lowWatermark = new AtomicLong(1L);
	
	
	private ByteBuffer assertBlock(int blockNumber) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * fills a block with the values in the supplied byte buffers
	 * it is the callers responsibility to acquire the block after writing
	 * 
	 * @param byteBuffers
	 * @return
	 */
	private long writeSlice(ByteBuffer[] byteBuffers) {
		int size = 0;
		for (int i = byteBuffers.length-1; i >=0; i--)
			size += byteBuffers[i].limit();

		long hwm;
		int offsetInBlock;
		int blockNumber;
		ByteBuffer buffer;
		synchronized(highWatermark) {
			hwm = highWatermark.get();
			offsetInBlock = (int)(hwm%BLOCK_MAX);
			blockNumber = (int)(hwm/BLOCK_MAX);
			
			// ensure space to write meta for 2 records (this one and the next):
			if (((hwm + size + 16)/BLOCK_MAX)!=blockNumber) {
				int remaining = BLOCK_MAX - offsetInBlock;
				ByteBuffer oldBlock = assertBlock(blockNumber);
				synchronized(oldBlock) {
					oldBlock.putInt(offsetInBlock, remaining);
					oldBlock.putInt(offsetInBlock+4,0);
				}
				hwm+=remaining;
				offsetInBlock = 0;
				blockNumber+=1;
			}

			// write the size and free marker first
			buffer = assertBlock(blockNumber);
			synchronized(buffer) {
				buffer.putInt(offsetInBlock,size+8);
				buffer.putInt(offsetInBlock+4,0);
			}
			
			// now that state is recoverable, set the highwatermark
			highWatermark.set(hwm+size+8);
		}
		synchronized (buffer) {
			// write the bytes outside of the HWM access
			buffer.position(offsetInBlock+8);
			for (int i=0; i < byteBuffers.length; i++) {
				byteBuffers[i].position(0);
				buffer.put(byteBuffers[i]);
			}
		}
		return hwm;
	}

	private ByteBuffer retrieveSliceAt(long physicalOffset) {
		if (physicalOffset==0) return EMPTY_BUFFER;
		int blockNumber = (int)(physicalOffset/BLOCK_MAX);
		int offsetInBlock = (int) (physicalOffset%BLOCK_MAX);
		ByteBuffer block = assertBlock(blockNumber);
		ByteBuffer retVal;
		int size;
		synchronized(block) {
			size = block.getInt(offsetInBlock)-8; 
			block.position(offsetInBlock+8);
			retVal = block.slice();
		}
		retVal.limit(size);
		return retVal.asReadOnlyBuffer();
	}

	private void acquireSliceAt(long physicalOffset) {
		if (physicalOffset==0) return;
		int blockNumber = (int)(physicalOffset/BLOCK_MAX);
		int offsetInBlock = (int) (physicalOffset%BLOCK_MAX);
		ByteBuffer block = assertBlock(blockNumber);
		synchronized (block) {
			block.putInt(offsetInBlock+4, block.getInt(offsetInBlock+4)+1 );
		}
	}

	private void releaseSliceAt(long physicalOffset) {
		if (physicalOffset==0) return;
		int blockNumber = (int)(physicalOffset/BLOCK_MAX);
		int offsetInBlock = (int) (physicalOffset%BLOCK_MAX);
		ByteBuffer block = assertBlock(blockNumber);
		int refCount;
		synchronized (block) {
			refCount = block.getInt(offsetInBlock+4)-1;
			block.putInt(offsetInBlock+4, refCount==0?-1:refCount); // 0 is a temporary "dont clean" value
		}
		if (refCount == 0) 
			raiseLowWaterMark(physicalOffset);
	}

	private void raiseLowWaterMark(long physicalOffset) {
		// successively read blocks, as long as their refCounts are < 0, raise the lwm
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
			acquireSliceAt(physicalOffset);
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
