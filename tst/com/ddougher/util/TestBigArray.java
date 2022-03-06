/**
 * 
 */
package com.ddougher.util;

import static org.junit.Assert.assertArrayEquals;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Random;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * @author Dana
 *
 */
@DisplayName("BigArray")
class TestBigArray {

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeAll
	static void setUpBeforeClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterAll
	static void tearDownAfterClass() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@BeforeEach
	void setUp() throws Exception {
	}

	/**
	 * @throws java.lang.Exception
	 */
	@AfterEach
	void tearDown() throws Exception {
	}

	@Test
	@DisplayName("Constructor Regression")
	void test10() {
		BigArrayImpl a = new BigArrayImpl(new MemoryAssetFactory());
		System.out.println(a.dump());
	}

	@Test
	@DisplayName("head insert")
	void test20() {
		BigArrayImpl a = new BigArrayImpl(new MemoryAssetFactory(), BigArrayImpl.defaultFragmentLimit, 15);
		long ts = System.currentTimeMillis();
		for (int i = 1; i < 50000; i++) {
 			a.insert(ByteBuffer.wrap(new byte [] { (byte)(i/10000), (byte)((i/1000)%10), (byte)((i/100)%10), (byte)((i/10)%10), (byte)(i%10) }), BigInteger.ZERO);
 			// System.out.println(a.dump());
		}
		System.out.println(System.currentTimeMillis()-ts);
	}
	
	@Test
	@DisplayName("tail insert")
	void test30() {
		BigArrayImpl a = new BigArrayImpl(new MemoryAssetFactory(), BigArrayImpl.defaultFragmentLimit, 15);
		long ts = System.currentTimeMillis();
		for (int i = 1; i < 50000; i++) {
 			a.insert(ByteBuffer.wrap(new byte [] { (byte)(i/10000), (byte)((i/1000)%10), (byte)((i/100)%10), (byte)((i/10)%10), (byte)(i%10) }), a.size());
 			// System.out.println(a.dump());
		}
		System.out.println(System.currentTimeMillis()-ts);
	}

	@Test
	@DisplayName("50k preloaded random insert")
	void test40() {
		Random r = new Random(0l);
		BigArray a = new BigArrayImpl(new MemoryAssetFactory(), BigArrayImpl.defaultFragmentLimit,17);
		Long ts;
		ts = System.currentTimeMillis();
		a.transact(ba->{
			for (int i = 1; i < 50000; i++) {
	 			ba.insert(ByteBuffer.wrap(new byte [] { (byte)(i/10000), (byte)((i/1000)%10), (byte)((i/100)%10), (byte)((i/10)%10), (byte)(i%10) }), ba.size());
	 			// System.out.println(a.dump());
			}
			return;
		});
		System.out.println(System.currentTimeMillis()-ts);
		ts = System.currentTimeMillis();
		a.transact(ba->{
			for (int i = 1; i < 50000; i++) {
	 			ba.insert(ByteBuffer.wrap(new byte [] { (byte)(i/10000), (byte)((i/1000)%10), (byte)((i/100)%10), (byte)((i/10)%10), (byte)(i%10) }), BigInteger.ZERO);
	 			// System.out.println(a.dump());
			}
			return;
		});
		System.out.println(System.currentTimeMillis()-ts);
		ts = System.currentTimeMillis();
		a.transact(ba->{
			for (int i = 1; i < 100000; i++) {
	 			ba.insert(ByteBuffer.wrap(new byte [] { (byte)(i/10000), (byte)((i/1000)%10), (byte)((i/100)%10), (byte)((i/10)%10), (byte)(i%10) }), BigInteger.valueOf(r.nextInt((ba.size().intValue()+5)/5)*5));
			}
			return;
		});
		System.out.println(System.currentTimeMillis()-ts);
	}
}
