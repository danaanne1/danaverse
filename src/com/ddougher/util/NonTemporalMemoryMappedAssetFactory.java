package com.ddougher.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

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
public class NonTemporalMemoryMappedAssetFactory implements AssetFactory, Serializable, Closeable {

	private static final long serialVersionUID = 1L;
	private static final int HWM_OFFSET = 0;
	private static final int LWM_OFFSET = 8;
	
	private final int BLOCK_MAX = Integer.MAX_VALUE;
	private final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	private final Map<Integer, RandomAccessFile> files = new HashMap<Integer, RandomAccessFile>();
	private final Map<Integer, MappedByteBuffer> blocks = new ConcurrentHashMap<Integer, MappedByteBuffer>();
	
	private transient RandomAccessFile metaFile;
	private transient MappedByteBuffer metaBuffer;

	private transient AtomicLong highWatermark = new AtomicLong(1L);
	private transient AtomicLong lowWatermark = new AtomicLong(1L);
	
	private File baseName = new File("data");
	private transient boolean closed = false;
	private transient ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
	
	
	public NonTemporalMemoryMappedAssetFactory(String basePath) throws IOException {
		baseName = new File(basePath);
		baseName.mkdirs();
		metaFile = new RandomAccessFile(new File(baseName,"meta"),"rw");
		metaFile.setLength(16);
		metaBuffer = metaFile.getChannel().map(MapMode.READ_WRITE, 0, 16);
	}

	@Override
	public void close() throws IOException {
		closeLock.writeLock().lock();
		try {
			if (closed) return;
			sync();
			files.values().parallelStream().forEach(raf-> { try { raf.close(); } catch (IOException e) {} });
			try { metaFile.close(); } catch (IOException e) { }
			blocks.clear();
			metaBuffer = null;
			files.clear();
		} finally {
			closed = true;
			closeLock.writeLock().unlock();
		}
		System.gc();
	}

	private void whenOpen(Runnable r) {
		whenOpen(()->{
			r.run();
			return null;
		});
	}
		
	private <T> T whenOpen(Supplier<T> s) {
		closeLock.readLock().lock();
		try {
			if (closed) throw new IllegalStateException("Factory is closed");
			return s.get();
		} finally { 
			closeLock.readLock().unlock();
		}
	}
		
	public void sync() {
		whenOpen(() -> {
			blocks.values().parallelStream().forEach(mmb -> { synchronized(mmb) {	mmb.force(); } } );
			metaBuffer.force();
		});
	}
	
	private void setHighWatermark(long hwm) {
		metaBuffer.putLong(HWM_OFFSET,hwm);
		highWatermark.set(hwm);
	}

	private long getHighWatermark() {
		return highWatermark.get();
	}
	private void setLowWatermark(long lwm) {
		metaBuffer.putLong(LWM_OFFSET,lwm);
		lowWatermark.set(lwm);
	}
	private long getLowWatermark() {
		return lowWatermark.get();
	}

	private ByteBuffer assertBlock(int blockNumber) {
		MappedByteBuffer block;
		if (null!=(block=blocks.get(blockNumber))) return block; 
		synchronized(files) {
			if (null!=(block=blocks.get(blockNumber))) return block;
			RandomAccessFile file = files.get(blockNumber/40);
			try {
				if (file == null) 
					files.put(blockNumber/40,file = new RandomAccessFile(new File(baseName,Integer.toString(blockNumber/40)), "rw"));
				long desiredLength = ((long)((blockNumber%40)+1))*BLOCK_MAX;
				if (file.length()<desiredLength)
					file.setLength(desiredLength);
				blocks.put(blockNumber, block = file.getChannel().map(MapMode.READ_WRITE, desiredLength-BLOCK_MAX, BLOCK_MAX));
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
		return block;
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
			hwm = getHighWatermark();
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
			setHighWatermark(hwm+size+8);
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
		long lwm = getLowWatermark();
		while (lwm < getHighWatermark() ) {
			ByteBuffer block = assertBlock((int) (lwm/BLOCK_MAX));
			synchronized(block) {
				if (block.getInt((int) ((lwm % BLOCK_MAX)+4))>=0)
					return;
				setLowWatermark(lwm=lwm+block.getInt((int) (lwm % BLOCK_MAX)));
			}
			// check for crossing a block boundary
			if ((lwm%BLOCK_MAX)==0) {
				blocks.remove((int)((lwm/BLOCK_MAX)-1));

				// additionally check for crossing a file boundary
				if ((lwm/BLOCK_MAX)%40==0) {
					synchronized (files) {
						int fileNum = (int) (((lwm/BLOCK_MAX)/40)-1);
						RandomAccessFile f = files.remove(fileNum);
						if (f!=null) {
							try {
								f.close();
							} catch (IOException e) {
								throw new RuntimeException(e);
							}
						}
					}
				}
			}
		}
	}


	class NonTemporalMemoryMappedAddressable implements Addressable {
		long physicalOffset = 0L;
		int size = 0;
		
		
		@Override
		public void set(ByteBuffer data, UUID tid) {
			whenOpen(()->{
				releaseSliceAt(physicalOffset);
				size = data.limit();
				physicalOffset = writeSlice(new ByteBuffer [] { data }); 
				acquireSliceAt(physicalOffset);
			});
		}

		@Override
		public void set(Addressable src, UUID tid) {
			whenOpen(()->{
				releaseSliceAt(physicalOffset);
				physicalOffset = ((NonTemporalMemoryMappedAddressable)src).physicalOffset;
				size = ((NonTemporalMemoryMappedAddressable)src).size;
				acquireSliceAt(physicalOffset);
			});
		}

		@Override
		public void append(Addressable a, UUID tid) {
			whenOpen(()->{
				long [] oldOffsets = { physicalOffset, ((NonTemporalMemoryMappedAddressable)a).physicalOffset };
				ByteBuffer [] buffers = {
						retrieveSliceAt(physicalOffset), 
						retrieveSliceAt(((NonTemporalMemoryMappedAddressable)a).physicalOffset) 
				};
				physicalOffset = writeSlice(buffers);
				size = buffers[0].limit() + buffers[1].limit();
				releaseSliceAt(oldOffsets[0]);
				releaseSliceAt(oldOffsets[1]);
			});
		}

		@Override
		public int size(UUID tid) {
			return size;
		}

		@Override
		public ByteBuffer get(UUID tid) {
			return whenOpen(()->{
				return retrieveSliceAt(physicalOffset);
			});
		}
		
	}
	
	
	@Override
	public Addressable createAddressable() {
		return new NonTemporalMemoryMappedAddressable();
	}


	@Override
	public Sizeable createSizeable() {
		// TODO Auto-generated method stub
		return null;
	}

}
