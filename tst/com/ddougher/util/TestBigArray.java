/**
 * 
 */
package com.ddougher.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
	@DisplayName("Head Loaded Space Walk Regression")
	void test20() throws InterruptedException {
		
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
	@DisplayName("Tail Loaded Space Walk Regression")
	void test30() throws InterruptedException {
		
		BigArray a = new BigArray(new MemoryAssetFactory(), p->{}, 0, BigInteger.ONE);
		a.insert(ByteBuffer.wrap(new byte [] { 1, 2, 3, 4, 5 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 22, 23 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 24 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 25 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 26 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 0 }), BigInteger.ZERO );
		a.insert(ByteBuffer.wrap(new byte [] { 18 }), BigInteger.valueOf(18) );
		a.insert(ByteBuffer.wrap(new byte [] { 21 }), BigInteger.valueOf(21) );
		int spaceCount = 0;
		while(!a.optimizeSpace()) {
			spaceCount++; 
		}
		assertEquals(6, spaceCount); // 3 actual, 3 no-ops
		assertEquals(("HWM: ed765fb2-7190-11ec-9d66-747827f83f3c\n"
				+ "FRE: 4\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 27, f: 4, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 19, f: 2, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 10, f: 1, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 6, f: 0, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 1, f: 0, p: -, n: 17, u: 16, o: [0]}\n"
				+ "                    R: 17 { s: 5, f: 0, p: 3, n: 9, u: 16, o: [1, 2, 3, 4, 5]}\n"
				+ "                R: 18 { s: 4, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 0, f: 1, p: 17, n: 19, u: 18, o: []}\n"
				+ "                    R: 19 { s: 4, f: 0, p: 9, n: 5, u: 18, o: [6, 7, 8, 9]}\n"
				+ "            R: 10 { s: 9, f: 1, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 3, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 0, f: 1, p: 19, n: 21, u: 20, o: []}\n"
				+ "                    R: 21 { s: 3, f: 0, p: 5, n: 11, u: 20, o: [10, 11, 12]}\n"
				+ "                R: 22 { s: 6, f: 0, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 5, f: 0, p: 21, n: 23, u: 22, o: [13, 14, 15, 16, 17]}\n"
				+ "                    R: 23 { s: 1, f: 0, p: 11, n: 2, u: 22, o: [18]}\n"
				+ "        R: 6 { s: 8, f: 2, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 5, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 0, f: 1, p: 23, n: 25, u: 24, o: []}\n"
				+ "                    R: 25 { s: 2, f: 0, p: 2, n: 13, u: 24, o: [19, 20]}\n"
				+ "                R: 26 { s: 3, f: 0, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 1, f: 0, p: 25, n: 27, u: 26, o: [21]}\n"
				+ "                    R: 27 { s: 2, f: 0, p: 13, n: 7, u: 26, o: [22, 23]}\n"
				+ "            R: 14 { s: 3, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 1, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 0, f: 1, p: 27, n: 29, u: 28, o: []}\n"
				+ "                    R: 29 { s: 1, f: 0, p: 7, n: 15, u: 28, o: [24]}\n"
				+ "                R: 30 { s: 2, f: 0, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 1, f: 0, p: 29, n: 31, u: 30, o: [25]}\n"
				+ "                    R: 31 { s: 1, f: 0, p: 15, n: -, u: 30, o: [26]}\n"
				+ "HEAD\n"
				+ "    3 { s: 1, f: 0, p: -, n: 17, u: 16, o: [0]}\n"
				+ "").substring(42), a.dump().substring(42));
	}
	
	@Test
	@DisplayName("Defragment Min Threshold Regression") 
	void test40() {
		BigArray a = new BigArray(new MemoryAssetFactory(), p->{}, 0, BigInteger.ONE);
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 1, 2, 3, 4, 5 }), BigInteger.ZERO);
		a.defragment();
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
	void test50() {
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
	
	@Test
	@DisplayName("Split Insert Regression")
	void test60() {
		BigArray a = new BigArray(new MemoryAssetFactory(), p->{}, Integer.MAX_VALUE, BigInteger.ONE);
		a.insert(ByteBuffer.wrap(new byte [] { 0, 2, 3, 4, 5 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 18 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 19, 21 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 22, 23 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 24 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 25 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 26 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 1 }), BigInteger.valueOf(1) );
		a.insert(ByteBuffer.wrap(new byte [] { 17 }), BigInteger.valueOf(17) );
		a.insert(ByteBuffer.wrap(new byte [] { 20 }), BigInteger.valueOf(20) );
		
		assertEquals(("HWM: ec8b66a1-7194-11ec-80a8-747827f83f3c\n"
				+ "FRE: 1\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 27, f: 1, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 18, f: 1, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 6, f: 1, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 2, f: 0, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 1, f: 0, p: -, n: 17, u: 16, o: [0]}\n"
				+ "                    R: 17 { s: 1, f: 0, p: 3, n: 9, u: 16, o: [1]}\n"
				+ "                R: 18 { s: 4, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 4, f: 0, p: 17, n: 19, u: 18, o: [2, 3, 4, 5]}\n"
				+ "                    R: 19 { s: 0, f: 1, p: 9, n: 5, u: 18, o: []}\n"
				+ "            R: 10 { s: 12, f: 0, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 7, f: 0, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 4, f: 0, p: 19, n: 21, u: 20, o: [6, 7, 8, 9]}\n"
				+ "                    R: 21 { s: 3, f: 0, p: 5, n: 11, u: 20, o: [10, 11, 12]}\n"
				+ "                R: 22 { s: 5, f: 0, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 4, f: 0, p: 21, n: 23, u: 22, o: [13, 14, 15, 16]}\n"
				+ "                    R: 23 { s: 1, f: 0, p: 11, n: 2, u: 22, o: [17]}\n"
				+ "        R: 6 { s: 9, f: 0, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 4, f: 0, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 2, f: 0, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 1, f: 0, p: 23, n: 25, u: 24, o: [18]}\n"
				+ "                    R: 25 { s: 1, f: 0, p: 2, n: 13, u: 24, o: [19]}\n"
				+ "                R: 26 { s: 2, f: 0, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 1, f: 0, p: 25, n: 27, u: 26, o: [20]}\n"
				+ "                    R: 27 { s: 1, f: 0, p: 13, n: 7, u: 26, o: [21]}\n"
				+ "            R: 14 { s: 5, f: 0, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 3, f: 0, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 2, f: 0, p: 27, n: 29, u: 28, o: [22, 23]}\n"
				+ "                    R: 29 { s: 1, f: 0, p: 7, n: 15, u: 28, o: [24]}\n"
				+ "                R: 30 { s: 2, f: 0, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 1, f: 0, p: 29, n: 31, u: 30, o: [25]}\n"
				+ "                    R: 31 { s: 1, f: 0, p: 15, n: -, u: 30, o: [26]}\n"
				+ "HEAD\n"
				+ "    3 { s: 1, f: 0, p: -, n: 17, u: 16, o: [0]}\n"
				+ "").substring(42), a.dump().substring(42));
	}

	
	@Test
	@DisplayName("Iteration Regression")
	void test70() {
		BigArray a = standardTestArray();

		ArrayList<byte[]> bb = new ArrayList<>();
		
		StreamSupport
			.stream(Spliterators.spliteratorUnknownSize(a.get(BigInteger.valueOf(1), BigInteger.valueOf(23)), Spliterator.ORDERED),false)
			.forEach((b)-> {
				byte [] buf = new byte[Math.min(20, b.limit())];
				((ByteBuffer) b.duplicate().rewind()).get(buf);
				synchronized(bb) { bb.add(buf); }
			});

		assertArrayEquals(new byte[][] { { 1 }, { 2, 3, 4, 5 }, { 6, 7, 8, 9, 10, 11, 12 }, { 13, 14, 15, 16, 17 }, { 18, 19 }, { 20, 21 }, { 22, 23 } }, bb.toArray());
	
	}
	
	@Test
	@DisplayName("Head Remove Regression")
	void test80() {
		BigArray a = standardTestArray();
		
		a.remove(BigInteger.ZERO, BigInteger.valueOf(2));

		assertEquals(("HWM: d0dba3b0-7253-11ec-8a37-747827f83f3c\n"
				+ "FRE: 9\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 25, f: 9, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 16, f: 5, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 4, f: 3, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 0, f: 2, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 0, f: 1, p: -, n: 17, u: 16, o: []}\n"
				+ "                    R: 17 { s: 0, f: 1, p: 3, n: 9, u: 16, o: []}\n"
				+ "                R: 18 { s: 4, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 4, f: 0, p: 17, n: 19, u: 18, o: [2, 3, 4, 5]}\n"
				+ "                    R: 19 { s: 0, f: 1, p: 9, n: 5, u: 18, o: []}\n"
				+ "            R: 10 { s: 12, f: 2, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 7, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 7, f: 0, p: 19, n: 21, u: 20, o: [6, 7, 8, 9, 10, 11, 12]}\n"
				+ "                    R: 21 { s: 0, f: 1, p: 5, n: 11, u: 20, o: []}\n"
				+ "                R: 22 { s: 5, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 5, f: 0, p: 21, n: 23, u: 22, o: [13, 14, 15, 16, 17]}\n"
				+ "                    R: 23 { s: 0, f: 1, p: 11, n: 2, u: 22, o: []}\n"
				+ "        R: 6 { s: 9, f: 4, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 4, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 2, f: 0, p: 23, n: 25, u: 24, o: [18, 19]}\n"
				+ "                    R: 25 { s: 0, f: 1, p: 2, n: 13, u: 24, o: []}\n"
				+ "                R: 26 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 2, f: 0, p: 25, n: 27, u: 26, o: [20, 21]}\n"
				+ "                    R: 27 { s: 0, f: 1, p: 13, n: 7, u: 26, o: []}\n"
				+ "            R: 14 { s: 5, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 3, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 3, f: 0, p: 27, n: 29, u: 28, o: [22, 23, 24]}\n"
				+ "                    R: 29 { s: 0, f: 1, p: 7, n: 15, u: 28, o: []}\n"
				+ "                R: 30 { s: 2, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 2, f: 0, p: 29, n: 31, u: 30, o: [25, 26]}\n"
				+ "                    R: 31 { s: 0, f: 1, p: 15, n: -, u: 30, o: []}\n"
				+ "HEAD\n"
				+ "    3 { s: 0, f: 1, p: -, n: 17, u: 16, o: []}\n"
				+ "").substring(42), a.dump().substring(42));
	}

	public BigArray standardTestArray() {
		BigArray a = new BigArray(new MemoryAssetFactory(), p->{}, Integer.MAX_VALUE, BigInteger.ONE);
		a.insert(ByteBuffer.wrap(new byte [] { 0, 2, 3, 4, 5 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 18 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 19, 21 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 22, 23 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 24 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 25 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 26 }), a.size());
		a.insert(ByteBuffer.wrap(new byte [] { 1 }), BigInteger.valueOf(1) );
		a.insert(ByteBuffer.wrap(new byte [] { 17 }), BigInteger.valueOf(17) );
		a.insert(ByteBuffer.wrap(new byte [] { 20 }), BigInteger.valueOf(20) );
		a.defragment();
		return a;
	}

	@Test
	@DisplayName("Tail Remove")
	void test82() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(27), BigInteger.valueOf(4)); // Purposefully overallocated

		assertEquals(("HWM: 5d3c5c3c-725b-11ec-85c9-747827f83f3c\n"
				+ "FRE: 8\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 27, f: 8, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 18, f: 4, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 6, f: 2, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 2, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "                    R: 17 { s: 0, f: 1, p: 3, n: 9, u: 16, o: []}\n"
				+ "                R: 18 { s: 4, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 4, f: 0, p: 17, n: 19, u: 18, o: [2, 3, 4, 5]}\n"
				+ "                    R: 19 { s: 0, f: 1, p: 9, n: 5, u: 18, o: []}\n"
				+ "            R: 10 { s: 12, f: 2, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 7, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 7, f: 0, p: 19, n: 21, u: 20, o: [6, 7, 8, 9, 10, 11, 12]}\n"
				+ "                    R: 21 { s: 0, f: 1, p: 5, n: 11, u: 20, o: []}\n"
				+ "                R: 22 { s: 5, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 5, f: 0, p: 21, n: 23, u: 22, o: [13, 14, 15, 16, 17]}\n"
				+ "                    R: 23 { s: 0, f: 1, p: 11, n: 2, u: 22, o: []}\n"
				+ "        R: 6 { s: 9, f: 4, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 4, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 2, f: 0, p: 23, n: 25, u: 24, o: [18, 19]}\n"
				+ "                    R: 25 { s: 0, f: 1, p: 2, n: 13, u: 24, o: []}\n"
				+ "                R: 26 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 2, f: 0, p: 25, n: 27, u: 26, o: [20, 21]}\n"
				+ "                    R: 27 { s: 0, f: 1, p: 13, n: 7, u: 26, o: []}\n"
				+ "            R: 14 { s: 5, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 3, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 3, f: 0, p: 27, n: 29, u: 28, o: [22, 23, 24]}\n"
				+ "                    R: 29 { s: 0, f: 1, p: 7, n: 15, u: 28, o: []}\n"
				+ "                R: 30 { s: 2, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 2, f: 0, p: 29, n: 31, u: 30, o: [25, 26]}\n"
				+ "                    R: 31 { s: 0, f: 1, p: 15, n: -, u: 30, o: []}\n"
				+ "HEAD\n"
				+ "    3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "").substring(42), a.dump().substring(42));
	}

	@Test
	@DisplayName("Remove (with GAPS)")
	void test84() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(2), BigInteger.valueOf(23));
		
		assertEquals(("HWM: 63470b70-7260-11ec-8111-747827f83f3c\n"
				+ "FRE: 13\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 7, f: 13, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 2, f: 7, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 2, f: 3, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 2, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "                    R: 17 { s: 0, f: 1, p: 3, n: 9, u: 16, o: []}\n"
				+ "                R: 18 { s: 0, f: 2, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 0, f: 1, p: 17, n: 19, u: 18, o: []}\n"
				+ "                    R: 19 { s: 0, f: 1, p: 9, n: 5, u: 18, o: []}\n"
				+ "            R: 10 { s: 0, f: 4, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 0, f: 2, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 0, f: 1, p: 19, n: 21, u: 20, o: []}\n"
				+ "                    R: 21 { s: 0, f: 1, p: 5, n: 11, u: 20, o: []}\n"
				+ "                R: 22 { s: 0, f: 2, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 0, f: 1, p: 21, n: 23, u: 22, o: []}\n"
				+ "                    R: 23 { s: 0, f: 1, p: 11, n: 2, u: 22, o: []}\n"
				+ "        R: 6 { s: 5, f: 6, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 0, f: 4, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 0, f: 2, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 0, f: 1, p: 23, n: 25, u: 24, o: []}\n"
				+ "                    R: 25 { s: 0, f: 1, p: 2, n: 13, u: 24, o: []}\n"
				+ "                R: 26 { s: 0, f: 2, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 0, f: 1, p: 25, n: 27, u: 26, o: []}\n"
				+ "                    R: 27 { s: 0, f: 1, p: 13, n: 7, u: 26, o: []}\n"
				+ "            R: 14 { s: 5, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 0, f: 2, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 0, f: 1, p: 27, n: 29, u: 28, o: []}\n"
				+ "                    R: 29 { s: 0, f: 1, p: 7, n: 15, u: 28, o: []}\n"
				+ "                R: 30 { s: 5, f: 0, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 2, f: 0, p: 29, n: 31, u: 30, o: [25, 26]}\n"
				+ "                    R: 31 { s: 3, f: 0, p: 15, n: -, u: 30, o: [27, 28, 29]}\n"
				+ "HEAD\n"
				+ "    3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "").substring(42), a.dump().substring(42));



	}
	
	
	@Test
	@DisplayName("Head Split Remove (with GAPS)")
	void test86() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		
		a.remove(BigInteger.valueOf(1), BigInteger.valueOf(17));

		assertEquals(("HWM: ee502769-7260-11ec-819a-747827f83f3c\n"
				+ "FRE: 10\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 13, f: 10, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 1, f: 7, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 1, f: 3, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 1, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 1, f: 0, p: -, n: 17, u: 16, o: [0]}\n"
				+ "                    R: 17 { s: 0, f: 1, p: 3, n: 9, u: 16, o: []}\n"
				+ "                R: 18 { s: 0, f: 2, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 0, f: 1, p: 17, n: 19, u: 18, o: []}\n"
				+ "                    R: 19 { s: 0, f: 1, p: 9, n: 5, u: 18, o: []}\n"
				+ "            R: 10 { s: 0, f: 4, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 0, f: 2, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 0, f: 1, p: 19, n: 21, u: 20, o: []}\n"
				+ "                    R: 21 { s: 0, f: 1, p: 5, n: 11, u: 20, o: []}\n"
				+ "                R: 22 { s: 0, f: 2, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 0, f: 1, p: 21, n: 23, u: 22, o: []}\n"
				+ "                    R: 23 { s: 0, f: 1, p: 11, n: 2, u: 22, o: []}\n"
				+ "        R: 6 { s: 12, f: 3, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 4, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 2, f: 0, p: 23, n: 25, u: 24, o: [18, 19]}\n"
				+ "                    R: 25 { s: 0, f: 1, p: 2, n: 13, u: 24, o: []}\n"
				+ "                R: 26 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 2, f: 0, p: 25, n: 27, u: 26, o: [20, 21]}\n"
				+ "                    R: 27 { s: 0, f: 1, p: 13, n: 7, u: 26, o: []}\n"
				+ "            R: 14 { s: 8, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 3, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 3, f: 0, p: 27, n: 29, u: 28, o: [22, 23, 24]}\n"
				+ "                    R: 29 { s: 0, f: 1, p: 7, n: 15, u: 28, o: []}\n"
				+ "                R: 30 { s: 5, f: 0, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 2, f: 0, p: 29, n: 31, u: 30, o: [25, 26]}\n"
				+ "                    R: 31 { s: 3, f: 0, p: 15, n: -, u: 30, o: [27, 28, 29]}\n"
				+ "HEAD\n"
				+ "    3 { s: 1, f: 0, p: -, n: 17, u: 16, o: [0]}\n"
				+ "").substring(42), a.dump().substring(42));
	}
	
	@Test
	@DisplayName("Tail Split Remove (with GAPS)")
	void test88() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(18), BigInteger.valueOf(10));

		assertEquals(("HWM: 4a7fb3c0-7261-11ec-928a-747827f83f3c\n"
				+ "FRE: 11\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 20, f: 11, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 18, f: 4, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 6, f: 2, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 2, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "                    R: 17 { s: 0, f: 1, p: 3, n: 9, u: 16, o: []}\n"
				+ "                R: 18 { s: 4, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 4, f: 0, p: 17, n: 19, u: 18, o: [2, 3, 4, 5]}\n"
				+ "                    R: 19 { s: 0, f: 1, p: 9, n: 5, u: 18, o: []}\n"
				+ "            R: 10 { s: 12, f: 2, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 7, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 7, f: 0, p: 19, n: 21, u: 20, o: [6, 7, 8, 9, 10, 11, 12]}\n"
				+ "                    R: 21 { s: 0, f: 1, p: 5, n: 11, u: 20, o: []}\n"
				+ "                R: 22 { s: 5, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 5, f: 0, p: 21, n: 23, u: 22, o: [13, 14, 15, 16, 17]}\n"
				+ "                    R: 23 { s: 0, f: 1, p: 11, n: 2, u: 22, o: []}\n"
				+ "        R: 6 { s: 2, f: 7, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 0, f: 4, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 0, f: 2, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 0, f: 1, p: 23, n: 25, u: 24, o: []}\n"
				+ "                    R: 25 { s: 0, f: 1, p: 2, n: 13, u: 24, o: []}\n"
				+ "                R: 26 { s: 0, f: 2, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 0, f: 1, p: 25, n: 27, u: 26, o: []}\n"
				+ "                    R: 27 { s: 0, f: 1, p: 13, n: 7, u: 26, o: []}\n"
				+ "            R: 14 { s: 2, f: 3, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 0, f: 2, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 0, f: 1, p: 27, n: 29, u: 28, o: []}\n"
				+ "                    R: 29 { s: 0, f: 1, p: 7, n: 15, u: 28, o: []}\n"
				+ "                R: 30 { s: 2, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 0, f: 1, p: 29, n: 31, u: 30, o: []}\n"
				+ "                    R: 31 { s: 2, f: 0, p: 15, n: -, u: 30, o: [28, 29]}\n"
				+ "HEAD\n"
				+ "    3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "").substring(42), a.dump().substring(42));
	}
	
	@Test
	@DisplayName("Dual Split Remove (with GAPS)")
	void test90() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(4), BigInteger.valueOf(11));

		assertEquals(("HWM: 361acbf3-7262-11ec-86a2-747827f83f3c\n"
				+ "FRE: 8\n"
				+ "DEP: 5\n"
				+ "ROOT\n"
				+ "    1 { s: 19, f: 8, p: -, n: -, u: -, o: null}\n"
				+ "        L: 4 { s: 7, f: 5, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 8 { s: 4, f: 2, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 16 { s: 2, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "                    R: 17 { s: 0, f: 1, p: 3, n: 9, u: 16, o: []}\n"
				+ "                R: 18 { s: 2, f: 1, p: -, n: -, u: 8, o: null}\n"
				+ "                    L: 9 { s: 2, f: 0, p: 17, n: 19, u: 18, o: [2, 3]}\n"
				+ "                    R: 19 { s: 0, f: 1, p: 9, n: 5, u: 18, o: []}\n"
				+ "            R: 10 { s: 3, f: 3, p: -, n: -, u: 4, o: null}\n"
				+ "                L: 20 { s: 0, f: 2, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 5 { s: 0, f: 1, p: 19, n: 21, u: 20, o: []}\n"
				+ "                    R: 21 { s: 0, f: 1, p: 5, n: 11, u: 20, o: []}\n"
				+ "                R: 22 { s: 3, f: 1, p: -, n: -, u: 10, o: null}\n"
				+ "                    L: 11 { s: 0, f: 1, p: 21, n: 23, u: 22, o: []}\n"
				+ "                    R: 23 { s: 3, f: 0, p: 11, n: 2, u: 22, o: [15, 16, 17]}\n"
				+ "        R: 6 { s: 12, f: 3, p: -, n: -, u: 1, o: null}\n"
				+ "            L: 12 { s: 4, f: 2, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 24 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 2 { s: 2, f: 0, p: 23, n: 25, u: 24, o: [18, 19]}\n"
				+ "                    R: 25 { s: 0, f: 1, p: 2, n: 13, u: 24, o: []}\n"
				+ "                R: 26 { s: 2, f: 1, p: -, n: -, u: 12, o: null}\n"
				+ "                    L: 13 { s: 2, f: 0, p: 25, n: 27, u: 26, o: [20, 21]}\n"
				+ "                    R: 27 { s: 0, f: 1, p: 13, n: 7, u: 26, o: []}\n"
				+ "            R: 14 { s: 8, f: 1, p: -, n: -, u: 6, o: null}\n"
				+ "                L: 28 { s: 3, f: 1, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 7 { s: 3, f: 0, p: 27, n: 29, u: 28, o: [22, 23, 24]}\n"
				+ "                    R: 29 { s: 0, f: 1, p: 7, n: 15, u: 28, o: []}\n"
				+ "                R: 30 { s: 5, f: 0, p: -, n: -, u: 14, o: null}\n"
				+ "                    L: 15 { s: 2, f: 0, p: 29, n: 31, u: 30, o: [25, 26]}\n"
				+ "                    R: 31 { s: 3, f: 0, p: 15, n: -, u: 30, o: [27, 28, 29]}\n"
				+ "HEAD\n"
				+ "    3 { s: 2, f: 0, p: -, n: 17, u: 16, o: [0, 1]}\n"
				+ "").substring(42), a.dump().substring(42));
	}
	
	
	@Test
	@DisplayName("Random Insert Performance")
	void test95() throws InterruptedException
	{
		Random r = new Random();
		BigArray a = null;
		a = new BigArray(new MemoryAssetFactory(), BigArray.defaultOptimizer, 0, BigInteger.valueOf(4) );
		for (int i = 0; i < 2000; i++) {
			a.insert(ByteBuffer.wrap(new byte [] { (byte)i }), BigInteger.valueOf(r.nextInt(a.size().intValue()+1)));
		}
		//System.out.println(a.dump());
	}
	
}
