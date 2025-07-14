package com.ddougher.buff.serialization;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Basic implementation of an Array for a lazy loading, convert on write, in memory serialization mechanism.
 * 
 * @author Dana
 */
public class Array {
	ByteBuffer backingBuffer;
	ArrayList<Record> records;

	public Array(ByteBuffer buffer) {
		this.backingBuffer = buffer;
	}

	public Array() {
		this(ByteBuffer.allocate(5).put(Types.ARRAY).putInt(0));
	}
	
	public Object set(int index, Object value) {
		materialize();
		Record old = records.set(index, new Record().withValue(value));
		if (old!=null)
			return old.value();
		return old;
	}
	
	public int size() {
		materialize();
		return records.size();
	}
	
	public void add(Object value) {
		materialize();
		records.add(new Record().withValue(value));
	}

	public void add(int index, Object value) {
		materialize();
		records.add(index, new Record().withValue(value));
	}
	
	public Object get(int index) {
		materialize();
		return records.get(index).value();
	}
	
	public Object remove(int index) {
		materialize();
		Record old = records.remove(index);
		if (old!=null)
			return old.value();
		return old;
	}

	public void clear() {
		records = new ArrayList<>();
	}
	
	private void materialize() {
		if (records!=null) return;
		
		records = new ArrayList<>();
		
		// read all of the sizes
		int [] sizes = new int[((ByteBuffer)backingBuffer.position(1)).getInt()];
		for (int i = 0; i < sizes.length; i++) {
			sizes[i] = backingBuffer.getInt();
		}

		// build the array
		for (int i = 0; i < sizes.length; i++) {
			records.add(new Record( (ByteBuffer) backingBuffer.slice().limit(sizes[i])));
			backingBuffer.position(backingBuffer.position()+sizes[i]);
		}
		
	}


	public ByteBuffer toByteBuffer() {
		if (records==null)
			return (ByteBuffer) backingBuffer.rewind();
		List<ByteBuffer> toWrite = records.stream().map(r->r.toByteBuffer()).collect(Collectors.toList());  
		
		// calculate size
		ByteBuffer result = ByteBuffer.allocate(
				1 //  type
				+ 4 // record count
				+ (toWrite.size() * 4) // size of all the sizes
				+ toWrite.stream().map(v->v.limit()).reduce(0,(a,b)->a+b) // size of all the buffers
		);

		// write the struct descriptor
		result.put(Types.ARRAY);
		
		// write the sizes
		result.putInt(toWrite.size());
		toWrite.forEach(buffer-> result.putInt(buffer.limit()));

		// write the buffers
		toWrite.forEach(buffer-> result.put((ByteBuffer) buffer.rewind()));
		
		return (ByteBuffer) result.rewind();
	}
	
	public String toString() {
		StringBuffer buffer = new StringBuffer("[");
		for (int i = 0; i < size(); i++)  {
			buffer.append(get(i).toString()).append(",");
		}
		buffer.deleteCharAt(buffer.length()-1);
		buffer.append("]");
		return buffer.toString();
	}

}
