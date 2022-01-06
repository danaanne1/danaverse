/**
 * 
 */
package com.ddougher.util;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * @author Dana
 *
 */
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
	void test1() {
		BigArray a = new BigArray(new MemoryAssetFactory());
		a.dump();
	}

	@Test
	void test2() {
		BigArray a = new BigArray(new MemoryAssetFactory());
		a.dump();
		a.insert(ByteBuffer.wrap(new byte [] { 1, 2, 3, 4, 5 }), BigInteger.ZERO);
		a.dump();
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
		a.dump();
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.dump();
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.dump();
		a.optimizeSpace();
		a.dump();
		a.optimizeSpace();
		a.dump();
		a.optimizeSpace();
		a.dump();
		a.optimizeSpace();
		a.dump();
	}

	
}
