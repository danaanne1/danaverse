package com.ddougher.util;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

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
		File f = new File("TestData");
		if (f.exists() && f.isDirectory()) {
			for (File d: f.listFiles())
				d.delete();
			f.delete();
		}
		factory = new NonTemporalMemoryMappedAssetFactory("TestData",2_000_000);
	}

	@AfterEach
	void tearDown() throws Exception {
		factory.close();
	}

	@Test
	void test() throws IOException {
		Addressable a = factory.createAddressable();
		a.set(ByteBuffer.wrap(new byte [] { 'h', 'e', 'l', 'l', 'o' }),null);
		ByteBuffer buf = a.get(null);
		byte [] b = new byte[buf.limit()];
		buf.position(0);
		buf.get(b);
		System.out.println(new String(b));
		System.out.println(new File("data").getCanonicalPath());
	}

}
