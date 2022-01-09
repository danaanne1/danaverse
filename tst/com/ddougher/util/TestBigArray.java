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
	void test1() {
		BigArray a = new BigArray(new MemoryAssetFactory());
		assertEquals(("HWM: 19283db0-7176-11ec-85f4-747827f83f3c\n"
				+ "FRE: 4\n"
				+ "DEP: 3\n"
				+ "ROOT\n"
				+ "    1 { s: 0, f: 4, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 0, f: 2, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 3 { s: 0, f: 1, p: -, n: 5, u: 4, o: []}\n"
				+ "            R: 5 { s: 0, f: 1, p: 3, n: 2, u: 4, o: []}\n"
				+ "        R: 6 { s: 0, f: 2, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 2 { s: 0, f: 1, p: 5, n: 7, u: 6, o: []}\n"
				+ "            R: 7 { s: 0, f: 1, p: 2, n: -, u: 6, o: []}\n"
				+ "HEAD\n"
				+ "    3 { s: 0, f: 1, p: -, n: 5, u: 4, o: []}\n"
				+ "").substring(42), a.dump().substring(42));
	}

	@Test
	@DisplayName("Space Walk Regression")
	void test2() throws InterruptedException {
		
		BigArray a = new BigArray(new MemoryAssetFactory(), p->{}, 0, BigInteger.ONE);
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
		int spaceCount = 0;
		while(!a.optimizeSpace()) spaceCount++; 
		assertEquals(3, spaceCount);
		assertEquals(("HWM: 65ad0ba3-7173-11ec-8997-747827f83f3c\n"
				+ "FRE: 4\n"
				+ "DEP: 4\n"
				+ "ROOT\n"
				+ "    1 { s: 15, f: 4, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 7, f: 2, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 4, f: 1, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 3 { s: 4, f: 0, p: -, n: 9, u: 8, o: [6, 7, 8, 9]}\n"
				+ "                R: 9 { s: 0, f: 1, p: 3, n: 5, u: 8, o: []}\n"
				+ "            R: 10 { s: 3, f: 1, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 5 { s: 3, f: 0, p: 9, n: 11, u: 10, o: [10, 11, 12]}\n"
				+ "                R: 11 { s: 0, f: 1, p: 5, n: 2, u: 10, o: []}\n"
				+ "        R: 6 { s: 8, f: 2, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 6, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 2 { s: 6, f: 0, p: 11, n: 13, u: 12, o: [13, 14, 15, 16, 17, 18]}\n"
				+ "                R: 13 { s: 0, f: 1, p: 2, n: 7, u: 12, o: []}\n"
				+ "            R: 14 { s: 2, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 7 { s: 2, f: 0, p: 13, n: 15, u: 14, o: [19, 20]}\n"
				+ "                R: 15 { s: 0, f: 1, p: 7, n: -, u: 14, o: []}\n"
				+ "HEAD\n"
				+ "    3 { s: 4, f: 0, p: -, n: 9, u: 8, o: [6, 7, 8, 9]}\n"
				+ "").substring(42), a.dump().substring(42));
	}

	
	@Test
	@DisplayName("Defragment Min Threshold Regression") 
	void test3() {
		BigArray a = new BigArray(new MemoryAssetFactory(), p->{}, 0, BigInteger.ONE);
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 1, 2, 3, 4, 5 }), BigInteger.ZERO);
		a.defragment();
		System.out.println(a.dump());
		assertEquals(("HWM: c86e31e3-7175-11ec-8a38-747827f83f3c\n"
				+ "FRE: 3\n"
				+ "DEP: 4\n"
				+ "ROOT\n"
				+ "    1 { s: 20, f: 3, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 18, f: 0, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 9, f: 0, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 3 { s: 5, f: 0, p: -, n: 9, u: 8, o: [1, 2, 3, 4, 5]}\n"
				+ "                R: 9 { s: 4, f: 0, p: 3, n: 5, u: 8, o: [6, 7, 8, 9]}\n"
				+ "            R: 10 { s: 9, f: 0, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 5 { s: 3, f: 0, p: 9, n: 11, u: 10, o: [10, 11, 12]}\n"
				+ "                R: 11 { s: 6, f: 0, p: 5, n: 2, u: 10, o: [13, 14, 15, 16, 17, 18]}\n"
				+ "        R: 6 { s: 2, f: 3, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 2, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 2 { s: 2, f: 0, p: 11, n: 13, u: 12, o: [19, 20]}\n"
				+ "                R: 13 { s: 0, f: 1, p: 2, n: 7, u: 12, o: []}\n"
				+ "            R: 14 { s: 0, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 7 { s: 0, f: 1, p: 13, n: 15, u: 14, o: []}\n"
				+ "                R: 15 { s: 0, f: 1, p: 7, n: -, u: 14, o: []}\n"
				+ "HEAD\n"
				+ "    3 { s: 5, f: 0, p: -, n: 9, u: 8, o: [1, 2, 3, 4, 5]}\n"
				+ "").substring(42), a.dump().substring(42));
	}
	
	@Test
	@DisplayName("Defragment Max Threshold Regression") 
	void test4() {
		BigArray a = new BigArray(new MemoryAssetFactory(), p->{}, Integer.MAX_VALUE, BigInteger.ONE);
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 1, 2, 3, 4, 5 }), BigInteger.ZERO);
		a.defragment();
		assertEquals(("HWM: 5965f859-7175-11ec-81b4-747827f83f3c\n"
				+ "FRE: 5\n"
				+ "DEP: 4\n"
				+ "ROOT\n"
				+ "    1 { s: 20, f: 5, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 18, f: 2, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 9, f: 1, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 3 { s: 9, f: 0, p: -, n: 9, u: 8, o: [1, 2, 3, 4, 5, 6, 7, 8, 9]}\n"
				+ "                R: 9 { s: 0, f: 1, p: 3, n: 5, u: 8, o: []}\n"
				+ "            R: 10 { s: 9, f: 1, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 5 { s: 9, f: 0, p: 9, n: 11, u: 10, o: [10, 11, 12, 13, 14, 15, 16, 17, 18]}\n"
				+ "                R: 11 { s: 0, f: 1, p: 5, n: 2, u: 10, o: []}\n"
				+ "        R: 6 { s: 2, f: 3, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 2, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 2 { s: 2, f: 0, p: 11, n: 13, u: 12, o: [19, 20]}\n"
				+ "                R: 13 { s: 0, f: 1, p: 2, n: 7, u: 12, o: []}\n"
				+ "            R: 14 { s: 0, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 7 { s: 0, f: 1, p: 13, n: 15, u: 14, o: []}\n"
				+ "                R: 15 { s: 0, f: 1, p: 7, n: -, u: 14, o: []}\n"
				+ "HEAD\n"
				+ "    3 { s: 9, f: 0, p: -, n: 9, u: 8, o: [1, 2, 3, 4, 5, 6, 7, 8, 9]}\n"
				+ "").substring(42), a.dump().substring(42));
	}

}
