package com.ddougher.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ddougher.util.BigArray.Addressable;

class TestNonTemporalMemoryMappedAssetFactory {

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	NonTemporalMemoryMappedAssetFactory factory;
	
	@BeforeEach
	void setUp() throws Exception {
		factory = new NonTemporalMemoryMappedAssetFactory("TestData",100_000);
	}

	@AfterEach
	void tearDown() throws Exception {
		factory.close();
		File f = new File("TestData");
		if (f.exists() && f.isDirectory()) {
			for (File d: f.listFiles())
				d.delete();
			f.delete();
		}
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
			factory = (NonTemporalMemoryMappedAssetFactory)o[0];
		}
		factory.close();	
	}

	@Test
	void test2() throws IOException, ClassNotFoundException {
		factory.close();
		factory = new NonTemporalMemoryMappedAssetFactory("TestData",100_000);

		Addressable [] addressables = new Addressable[100000];
		for (int i=100000; i>0; i--) {
			String s = i + " bottles of beer on the wall " + i + " bottles of beer, take 1 down, pass it around " + (i-1) + " bottles of beer on the wall";
			addressables[i-1] = factory.createAddressable(ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)), null);
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
			factory = (NonTemporalMemoryMappedAssetFactory)o[0];
			addressables = (Addressable[])o[1];
		}
		
		for (int i=100000; i>0; i--) {
			String s = i + " bottles of beer on the wall " + i + " bottles of beer, take 1 down, pass it around " + (i-1) + " bottles of beer on the wall";
			ByteBuffer buf = addressables[i-1].get(null);
			byte [] b = new byte[buf.limit()];
			buf.position(0);
			buf.get(b);
			assertEquals(s, new String(b,StandardCharsets.UTF_8));
		}
	}

}
