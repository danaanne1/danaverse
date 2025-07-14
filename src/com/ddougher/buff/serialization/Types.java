package com.ddougher.buff.serialization;

import com.theunknowablebits.buff.serialization.Array;
import com.theunknowablebits.buff.serialization.Struct;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * A container for type conversion methods and constants.
 * @author Dana
 */
public final class Types {

	public static final byte STRUCT = 0x01;
	public static final byte ARRAY = 0x02;
	public static final byte BYTE = 0x03;
	public static final byte CHAR = 0x04;
	public static final byte DOUBLE = 0x05;
	public static final byte FLOAT = 0x06;
	public static final byte INT = 0x07;
	public static final byte LONG = 0x08;
	public static final byte SHORT = 0x09;
	public static final byte STRING = 0x10;
	public static final byte OBJECT = 0x11;
	public static final byte NULL = 0x12;
	
	public static final ByteBuffer NULL_BUFFER = ByteBuffer.allocate(1).put(NULL);
	
	private static final Map<Class<?>,Function<Object, ByteBuffer>> _writers = new HashMap<>();
	private static final Map<Byte,Function<ByteBuffer, Object>> _readers = new HashMap<>();
	
	static {
		_writers.put(Struct.class, v->((Struct)v).toByteBuffer());
		_writers.put(Array.class, v->((Array)v).toByteBuffer());
		_writers.put(Byte.class, v->(ByteBuffer)ByteBuffer.allocate(2).put(BYTE).put((Byte)v).rewind());
		_writers.put(Character.class, v->(ByteBuffer)ByteBuffer.allocate(3).put(CHAR).putChar((Character)v).rewind());
		_writers.put(Double.class, v->(ByteBuffer)ByteBuffer.allocate(9).put(DOUBLE).putDouble((Double)v).rewind());
		_writers.put(Float.class, v->(ByteBuffer)ByteBuffer.allocate(5).put(FLOAT).putFloat((Float)v).rewind());
		_writers.put(Integer.class, v->(ByteBuffer)ByteBuffer.allocate(5).put(INT).putInt((Integer)v).rewind());
		_writers.put(Long.class, v->(ByteBuffer)ByteBuffer.allocate(9).put(LONG).putLong((Long)v).rewind());
		_writers.put(Short.class, v->(ByteBuffer)ByteBuffer.allocate(3).put(SHORT).putShort((Short)v).rewind());
		_writers.put(Byte.TYPE, _writers.get(Byte.class));
		_writers.put(Character.TYPE, _writers.get(Character.class));
		_writers.put(Double.TYPE, _writers.get(Double.class));
		_writers.put(Float.TYPE, _writers.get(Float.class));
		_writers.put(Integer.TYPE, _writers.get(Integer.class));
		_writers.put(Long.TYPE, _writers.get(Long.class));
		_writers.put(Short.TYPE, _writers.get(Short.class));
		_writers.put(String.class, v-> {
			ByteBuffer b = ByteBuffer.allocate(((String)v).length()*2+1).put(STRING);
			b.asCharBuffer().put((String)v);
			return (ByteBuffer)b.rewind();
		});
		_writers.put(Object.class, Types::writeSerializedObjectToBuffer);
		
		_readers.put(STRUCT, b->new Struct(b));
		_readers.put(ARRAY, b->new Array(b));
		_readers.put(BYTE, b->b.get(1));
		_readers.put(CHAR, b->b.getChar(1));
		_readers.put(DOUBLE, b->b.getDouble(1));
		_readers.put(FLOAT, b->b.getFloat(1));
		_readers.put(INT, b->b.getInt(1));
		_readers.put(LONG, b->b.getLong(1));
		_readers.put(SHORT, b->b.getShort(1));
		_readers.put(STRING, b->((ByteBuffer)b.position(1)).asCharBuffer().toString());
		_readers.put(OBJECT, Types::readSerializedObjectFromBuffer);
		_readers.put(NULL, b->null);
	}

	private static final Object readSerializedObjectFromBuffer(ByteBuffer buffer) {
		byte [] b = new byte[buffer.limit()-1];
		// int position = buffer.position();
		buffer.position(1);
		buffer.get(b);
		// buffer.position(position);
		try ( ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(b)) ) {
			return oin.readObject();
		} catch (ClassNotFoundException | IOException e ) {
			throw new RuntimeException(e);
		}
	}

	private static final ByteBuffer writeSerializedObjectToBuffer(Object obj) {
		try (	ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ObjectOutputStream oout = new ObjectOutputStream(bout) ) {
			oout.writeObject(obj);
			oout.close();
			byte [] b = bout.toByteArray();
			return ByteBuffer.allocate(b.length+1).put(OBJECT).put(b);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public final static ByteBuffer toByteBuffer(Object obj) {
		if (obj==null)
			return NULL_BUFFER;
		if (_writers.containsKey(obj.getClass())) {
			return _writers.get(obj.getClass()).apply(obj);
		}
		return _writers.get(Object.class).apply(obj);
	}
	
	public final static Object fromByteBuffer(ByteBuffer buf) {
		return _readers.get(buf.get(0)).apply(buf);
	}

}