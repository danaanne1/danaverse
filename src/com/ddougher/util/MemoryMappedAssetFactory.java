package com.ddougher.util;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

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
public class MemoryMappedAssetFactory implements AssetFactory, Serializable, Closeable {

	private static final long serialVersionUID = 2L;
	
	private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);
	
	private transient Map<Integer, RandomAccessFile> files;
	private transient Map<Integer, MappedByteBuffer> blocks;
	private transient ConcurrentLinkedQueue<Integer> oldFileNumbers;
	private transient Object cleanupMonitor;
	private transient Thread cleanerThread;
	private transient volatile boolean active;
	private transient boolean closed;
	
	private File baseFile;
	private final int BLOCK_MAX;
	
	private AtomicLong highWatermark = new AtomicLong(1L);
	private AtomicLong lowWatermark = new AtomicLong(1L); 
	private ReentrantReadWriteLock closeLock = new ReentrantReadWriteLock();
	private ReentrantLock watermarkLock = new ReentrantLock();
	
	
	/**
	 * Initializes the meta file structures. This will only ever be called from inside whenOpen().
	 */
	private void initMeta() {
		if (!active) {
			synchronized(this) {
				if (!active) {
					baseFile.mkdirs();
					active = true;
					cleanerThread = new Thread(this::cleanupFiles, baseFile.getName()+" file cleanup");
					cleanerThread.start();
				}
			}
		}
	}
	
	public MemoryMappedAssetFactory() {
		this(Optional.empty(), Optional.empty());
	}
	
	public MemoryMappedAssetFactory(Optional<String> basePath, Optional<Integer> maxBlockSize) {
		BLOCK_MAX = maxBlockSize.orElse(Integer.MAX_VALUE);
		baseFile = new File(basePath.orElse("data"));
		initTransients();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		initTransients();
	}
	
	private void initTransients() {
		files = new HashMap<Integer, RandomAccessFile>();
		blocks = new ConcurrentHashMap<Integer, MappedByteBuffer>();
		oldFileNumbers = new ConcurrentLinkedQueue<Integer>();
		cleanupMonitor = new Object();
		active = false;
		closed = false;
	}

	
	// this is really messy but does the job. TODO clean this up and make it nice
	private void cleanupFiles() {
		int j = 0;
		while (active || (j < 1000 && !oldFileNumbers.isEmpty())) {
			synchronized (cleanupMonitor) {
				try {
					if (active) cleanupMonitor.wait(10000);
					LinkedList<Integer> toRemove = new LinkedList<Integer>();
					oldFileNumbers.forEach(fn-> {
						File f = new File(baseFile, Integer.toString(fn));
						if (f.exists())
							f.delete();
						else
							toRemove.add(fn);
					});
					oldFileNumbers.removeAll(toRemove);
				} catch (InterruptedException e) {
				}
			}
			if (!active) {
				if (oldFileNumbers.isEmpty())
					return;
				System.gc();
				j++;
			}
		}
	};

	
	@Override
	public void close() throws IOException {
		closeLock.writeLock().lock();
		try {
			if (closed) return;
			if (active) {
				sync();
				files.values().parallelStream().forEach(raf-> { try { raf.close(); } catch (IOException e) {} });
				blocks.clear();
				files.clear();
				active = false;
				synchronized(cleanupMonitor) {
					cleanupMonitor.notifyAll();
				}
				cleanerThread.join();
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		} finally {
			closed = true;
			closeLock.writeLock().unlock();
		}
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
			initMeta();
			return s.get();
		} finally { 
			closeLock.readLock().unlock();
		}
	}
		
	public void sync() {
		whenOpen(() -> {
			blocks.values().parallelStream().forEach(mmb -> { synchronized(mmb) {	mmb.force(); } } );
		});
	}
	
	private ByteBuffer assertBlock(int blockNumber) {
		MappedByteBuffer block;
		if (null!=(block=blocks.get(blockNumber))) return block; 
		synchronized(files) {
			if (null!=(block=blocks.get(blockNumber))) return block;
			RandomAccessFile file = files.get(blockNumber/40);
			try {
				if (file == null) 
					files.put(blockNumber/40,file = new RandomAccessFile(new File(baseFile,Integer.toString(blockNumber/40)), "rw"));
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
			hwm = highWatermark.get();
			offsetInBlock = (int)(hwm%BLOCK_MAX);
			blockNumber = (int)(hwm/BLOCK_MAX);
			
			// ensure space to write meta for 2 records (this one and the next):
			if (((hwm + size + 16)/BLOCK_MAX)!=blockNumber) {
				int remaining = BLOCK_MAX - offsetInBlock;
				ByteBuffer oldBlock = assertBlock(blockNumber);
				synchronized(oldBlock) {
					oldBlock.putInt(offsetInBlock, remaining);
					oldBlock.putInt(offsetInBlock+4,-1);
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
		if (watermarkLock.tryLock()) {
			try {
				long lwm = lowWatermark.get();
				while (lwm < highWatermark.get() ) {
					ByteBuffer block = assertBlock((int) (lwm/BLOCK_MAX));
					synchronized(block) {
						if (block.getInt((int) ((lwm % BLOCK_MAX)+4))>=0)
							return;
						lowWatermark.set(lwm=lwm+block.getInt((int) (lwm % BLOCK_MAX)));
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
										oldFileNumbers.add(fileNum);
									} catch (Exception e) {
										throw new RuntimeException(e);
									}
								}
							}
						}
					}
				}
			} finally {
				watermarkLock.unlock();
			}
		}
	}


	private class MemoryMappedAddressable implements Addressable, Serializable {
		private static final long serialVersionUID = 1L;
		long physicalOffset = 0L;
		int size = 0;
		
		@Override
		public void set(ByteBuffer data) {
			whenOpen(()->{
				releaseSliceAt(physicalOffset);
				if (data==null ||data.limit()<=0) {
					size = 0;
					physicalOffset = 0L;
				} else {
					size = data.limit();
					physicalOffset = writeSlice(new ByteBuffer [] { data }); 
					acquireSliceAt(physicalOffset);
				}
			});
		}

		@Override
		public void set(Addressable src) {
			whenOpen(()->{
				releaseSliceAt(physicalOffset);
				physicalOffset = ((MemoryMappedAddressable)src).physicalOffset;
				size = ((MemoryMappedAddressable)src).size;
				acquireSliceAt(physicalOffset);
			});
		}

		@Override
		public void append(Addressable a) {
			whenOpen(()->{
				long [] oldOffsets = { physicalOffset, ((MemoryMappedAddressable)a).physicalOffset };
				ByteBuffer [] buffers = {
						retrieveSliceAt(physicalOffset), 
						retrieveSliceAt(((MemoryMappedAddressable)a).physicalOffset) 
				};
				physicalOffset = writeSlice(buffers);
				size = buffers[0].limit() + buffers[1].limit();
				releaseSliceAt(oldOffsets[0]);
				releaseSliceAt(oldOffsets[1]);
			});
		}

		@Override
		public int size() {
			return size;
		}

		@Override
		public ByteBuffer get() {
			return whenOpen(()->{
				return retrieveSliceAt(physicalOffset);
			});
		}

		@Override
		public AssetFactory factory() {
			return MemoryMappedAssetFactory.this;
		}
		
	}
	
	
	@Override
	public Addressable createAddressable() {
		return new MemoryMappedAddressable();
	}


}
