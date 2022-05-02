package com.ddougher.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ddougher.util.AssetFactory.Addressable;


class TestMemoryMappedAssetFactory {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	MemoryMappedAssetFactory factory;
	
	@BeforeEach
	void setUp() throws Exception {
		File f = new File("TestData");
		if (f.exists() && f.isDirectory()) {
			for (File d: f.listFiles())
				d.delete();
			f.delete();
		}
		factory = new MemoryMappedAssetFactory(Optional.of("TestData"),Optional.of(100_000));
	}

	@AfterEach
	void tearDown() throws Exception {
		factory.close();
	}

	@Test
	void test1() throws IOException, ClassNotFoundException {
		factory.close();  
		byte [] sBytes;
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ObjectOutputStream oout = new ObjectOutputStream(bout) ) {
			oout.writeObject(new Object [] { factory });
			oout.flush();
			bout.flush();
			sBytes = bout.toByteArray();
		}

		try (ByteArrayInputStream bin = new ByteArrayInputStream(sBytes);
				ObjectInputStream oin = new ObjectInputStream(bin)) {
			Object [] o = (Object [])oin.readObject();
			factory = (MemoryMappedAssetFactory)o[0];
		}
		factory.close();	
	}

	@Test
	void test2() throws IOException, ClassNotFoundException {
		factory.close();
		factory = new MemoryMappedAssetFactory(Optional.of("TestData"),Optional.of(100_000));

		Addressable [] addressables = new Addressable[100000];
		for (int i=100000; i>0; i--) {
			String s = i + " bottles of beer on the wall " + i + " bottles of beer, take 1 down, pass it around " + (i-1) + " bottles of beer on the wall";
			addressables[i-1] = factory.createAddressable(ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)));
		}
		factory.close();  
		byte [] sBytes;
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ObjectOutputStream oout = new ObjectOutputStream(bout) ) {
			oout.writeObject(new Object [] { factory, addressables });
			oout.flush();
			bout.flush();
			sBytes = bout.toByteArray();
		}

		try (ByteArrayInputStream bin = new ByteArrayInputStream(sBytes);
				ObjectInputStream oin = new ObjectInputStream(bin)) {
			Object [] o = (Object [])oin.readObject();
			factory = (MemoryMappedAssetFactory)o[0];
			addressables = (Addressable[])o[1];
		}
		
		for (int i=100000; i>0; i--) {
			String s = i + " bottles of beer on the wall " + i + " bottles of beer, take 1 down, pass it around " + (i-1) + " bottles of beer on the wall";
			ByteBuffer buf = addressables[i-1].get();
			byte [] b = new byte[buf.limit()];
			buf.position(0);
			buf.get(b);
			assertEquals(s, new String(b,StandardCharsets.UTF_8));
		}
	}

	@Test
	void test3() throws IOException, ClassNotFoundException {
		factory.close();
		factory = new MemoryMappedAssetFactory(Optional.of("TestData"),Optional.of(100_000));

		Addressable [] addressables = new Addressable[100000];
		for (int i=100000; i>0; i--) {
			String s = i + " bottles of beer on the wall " + i + " bottles of beer, take 1 down, pass it around " + (i-1) + " bottles of beer on the wall";
			addressables[i-1] = factory.createAddressable(ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)));
		}
		// roll them
		for (int i = 0; i < 100000; i++) {
			addressables[i].set(addressables[i].get());
		}
		factory.close();  
		byte [] sBytes;
		try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
				ObjectOutputStream oout = new ObjectOutputStream(bout) ) {
			oout.writeObject(new Object [] { factory, addressables });
			oout.flush();
			bout.flush();
			sBytes = bout.toByteArray();
		}

		try (ByteArrayInputStream bin = new ByteArrayInputStream(sBytes);
				ObjectInputStream oin = new ObjectInputStream(bin)) {
			Object [] o = (Object [])oin.readObject();
			factory = (MemoryMappedAssetFactory)o[0];
			addressables = (Addressable[])o[1];
		}
		
		for (int i=100000; i>0; i--) {
			String s = i + " bottles of beer on the wall " + i + " bottles of beer, take 1 down, pass it around " + (i-1) + " bottles of beer on the wall";
			ByteBuffer buf = addressables[i-1].get();
			byte [] b = new byte[buf.limit()];
			buf.position(0);
			buf.get(b);
			assertEquals(s, new String(b,StandardCharsets.UTF_8));
		}
	}
}
