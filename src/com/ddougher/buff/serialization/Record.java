package com.ddougher.buff.serialization;

import java.nio.ByteBuffer;

/** 
 * A Lazy Loading Byte Buffer Record 
 */
public class Record {
	private static final Object OBJECT_MISSING = new Object();
	private ByteBuffer buffer;
	private Object value;
	public Record(ByteBuffer buffer) {
		this.buffer = buffer;
		this.value = OBJECT_MISSING;
	}
	public Record() {
		this.buffer = Types.NULL_BUFFER;
		this.value = null;
	}
	public Record withValue(Object value) {
		this.value = value;
		return this;
	}
	public Object value() {
		if (value==OBJECT_MISSING)
			value = Types.fromByteBuffer(buffer);
		return value;
	}
	public boolean isNull() {
		return value==null;
	}
	public ByteBuffer toByteBuffer() {
		if (value==OBJECT_MISSING)
			return buffer;
		return Types.toByteBuffer(value);
	}
}
