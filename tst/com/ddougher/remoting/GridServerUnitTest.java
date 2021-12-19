package com.ddougher.remoting;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.theunknowablebits.proxamic.DocumentStore;
import com.theunknowablebits.proxamic.MemoryDocumentStore;
import com.theunknowablebits.proxamic.exampledata.AbilityScore;
import com.theunknowablebits.proxamic.exampledata.CharacterRecord;

class GridServerUnitTest {

	transient GridServer server;
	transient GridClient client;
	
	@BeforeEach
	void setUp() throws Exception {
		InetSocketAddress serverAddress = new InetSocketAddress(0);
		server = new GridServer(serverAddress);
		server.start();
		client = new GridClient(server.getBoundAddress());
		client.start();
	}

	@AfterEach
	void tearDown() throws Exception {
		client.close();
		server.shutdown();
	}

	@Test
	void toasterTest() throws IOException, InterruptedException, ExecutionException {
		TestService service = client.createRemoteObject(TestService.class, TestServiceImpl.class, new Class[0], new Object[0]);
		Assert.assertNotEquals(Thread.currentThread().getId(), service.getThreadId());
	}

	@Test
	void testParallel() throws IOException, InterruptedException, ExecutionException {
		TestService service = client.createRemoteObject(TestService.class, TestServiceImpl.class, new Class[0], new Object[0]);
		Object[] result=Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 ).parallelStream().map(service::add).toArray();
		for (Object o: result) {
			if (o==(Object)105) return;
		}
		Assert.fail("Expected 105 to be present but got " + Arrays.toString(result));
	}

	@Test
	void testRemoteDocumentStore() throws IOException, InterruptedException, ExecutionException {
		DocumentStore store = client.createRemoteObject(DocumentStore.class, MemoryDocumentStore.class, new Class[0], new Object[0]);
		CharacterRecord record = store.get(CharacterRecord.class, "Dana");
		record.setName("The Engineer");
		record.setAge(BigDecimal.ZERO);
		record.setLevel(100);
		for (Object [] ob: new Object [][] { { "STR", 12 }, {"INT", 21 }, {"DEX", 19}, {"WIS", 16}, {"CON", 16}, {"CHR", 16} } ) {
			AbilityScore score=store.newInstance(AbilityScore.class).withName((String)ob[0]).withValue((int)ob[1]);
			record.abilityScoreList().add(score);
			record.abilityScoreMap().put(score.name(), score);
		}
		store.put(record); 
		record = store.get(CharacterRecord.class, "Dana");
		Assert.assertEquals("The Engineer", record.name());
		Assert.assertEquals(BigDecimal.ZERO, record.getAge());
		Assert.assertEquals((Integer)100, record.getLevel());
		Assert.assertEquals((Integer)21, record.abilityScoreMap().get("INT").value());
	}
	
	public static class TestServiceImpl implements TestService {
		
		AtomicInteger accumulator = new AtomicInteger();
		
		@Override
		public int add(int value) {
			return accumulator.addAndGet(value);
		}
		
		@Override
		public long getThreadId()
		{
			return Thread.currentThread().getId();
		}
		
	
	}
	
	public static interface TestService {
		long getThreadId();
		int add(int value);
	}
	
	
}
