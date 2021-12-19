package com.ddougher.remoting;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.jupiter.api.*;

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
	void addSomeNumbers() throws IOException, InterruptedException, ExecutionException {
		TestService service = client.createRemoteObject(TestService.class, TestServiceImpl.class, new Class[0], new Object[0]);
		
		System.out.println(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 ).stream().map(service::add).toArray());
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
