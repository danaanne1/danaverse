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
		BigArray a = new BigArray(new MemoryAssetFactory());
	}

	@Test
	@DisplayName("Head Loaded Space Walk Regression")
	void test20() throws InterruptedException {
		
		BigArray a = new BigArray(new MemoryAssetFactory(), 0);
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
	}

	@Test
	@DisplayName("Tail Loaded Space Walk Regression")
	void test30() throws InterruptedException {
		
		BigArray a = new BigArray(new MemoryAssetFactory(), 0);
		
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
	}
	
	@Test
	@DisplayName("Defragment Min Threshold Regression") 
	void test40() {
		BigArray a = new BigArray(new MemoryAssetFactory(), 0);
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 1, 2, 3, 4, 5 }), BigInteger.ZERO);
	}
	
	@Test
	@DisplayName("Defragment Max Threshold Regression") 
	void test50() {
		BigArray a = new BigArray(new MemoryAssetFactory(), Integer.MAX_VALUE);
		a.insert(ByteBuffer.wrap(new byte [] { 19, 20 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 13, 14, 15, 16, 17, 18 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 10, 11, 12 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 6, 7, 8, 9 }), BigInteger.ZERO);
		a.insert(ByteBuffer.wrap(new byte [] { 1, 2, 3, 4, 5 }), BigInteger.ZERO);
	}
	
	@Test
	@DisplayName("Split Insert Regression")
	void test60() {
		BigArray a = new BigArray(new MemoryAssetFactory(), Integer.MAX_VALUE);
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

	}

	public BigArray standardTestArray() {
		BigArray a = new BigArray(new MemoryAssetFactory(), Integer.MAX_VALUE);
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
		return a;
	}

	@Test
	@DisplayName("Tail Remove")
	void test82() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(27), BigInteger.valueOf(4)); // Purposefully overallocated

	}

	@Test
	@DisplayName("Remove (with GAPS)")
	void test84() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(2), BigInteger.valueOf(23));
		
	}
	
	
	@Test
	@DisplayName("Head Split Remove (with GAPS)")
	void test86() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		
		a.remove(BigInteger.valueOf(1), BigInteger.valueOf(17));

	}
	
	@Test
	@DisplayName("Tail Split Remove (with GAPS)")
	void test88() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(18), BigInteger.valueOf(10));

	}
	
	@Test
	@DisplayName("Dual Split Remove (with GAPS)")
	void test90() {
		BigArray a = standardTestArray();
		a.insert(ByteBuffer.wrap(new byte [] { 27, 28, 29 }), BigInteger.valueOf(27));
		a.remove(BigInteger.valueOf(4), BigInteger.valueOf(11));

	}
	
	
	@Test
	@DisplayName("Random Insert Performance")
	void test95() throws InterruptedException
	{
		Random r = new Random();
		BigArray a = null;
		for(long j : new long [] {2000, 4000, 6000, 8000, 8000, 8000, 10000, 40000 }) {
			long time = System.currentTimeMillis();
			a = new BigArray(new MemoryAssetFactory(),  0 );
			for (int i = 0; i < j; i++) {
				a.insert(ByteBuffer.wrap(new byte [] { (byte)i }), BigInteger.valueOf(r.nextInt(a.size().intValue()+1)));
			}
			System.out.println("--------------------------------------------------------------");
			System.out.println(System.currentTimeMillis()-time);
			a.dumpMetrics();
		}
		//System.out.println(a.dump());
	}
	
}
