package com.ddougher.util;

import java.io.File;
import java.io.RandomAccessFile;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.ddougher.util.BigArray.Addressable;
import com.ddougher.util.BigArray.AssetFactory;
import com.ddougher.util.BigArray.Sizeable;

/*
 * Must support serialization via a recreation strategy
 * Must support embedded size and reference counting
 * 
 * This is WORM strategy ONLY.
 * 
 * block = physicalOffset / Integer.MAX_INT
 * file_number = block/40
 * 
 */
public class SharedMemoryAssetFactory implements AssetFactory {

	
	
	final Map<Integer, RandomAccessFile> files = new HashMap<Integer, RandomAccessFile>();
	final Map<Integer, MappedByteBuffer> blocks = new HashMap<Integer, MappedByteBuffer>();

	File baseName;
	long highWatermark;
	long lowWatermark;
	
	// need locks for the HWM and LWM values
	
	
	class SharedMemoryAddressable implements Addressable {
		long physicalOffset;
		int size;
		
		@Override
		public void set(ByteBuffer data, UUID tid) {
			
			
		}

		@Override
		public void set(Addressable src, UUID tid) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void append(Addressable a, UUID tid) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public int size(UUID tid) {
			// TODO Auto-generated method stub
			return 0;
		}

		@Override
		public ByteBuffer get(UUID tid) {
			// TODO Auto-generated method stub
			return null;
		}
		
	}
	
	
	@Override
	public Addressable createAddressable() {
		
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Sizeable createSizeable() {
		// TODO Auto-generated method stub
		return null;
	}

}
