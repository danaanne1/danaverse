package com.ddougher.remoting;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Parallel Processing is the new frontier. Doing it smoothly, transparently, and seamlessly... is the vehicle that will get us across.
 * <p>
 * This is not your grandmas task/job execution engine based pile of crap. This is real software created by real geniuses for
 * accomplishing real things.
 * <p>
 * 
 * @author Dana
 *
 */
public class GridServer implements Runnable, Closeable {

	private Selector selector;
	private ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	private volatile boolean closed = false;

	public GridServer() throws IOException {
		selector = Selector.open();
	}

	public void run() {
		if (closed) throw new IllegalStateException();
		while (!closed) {
			try {
				if (selector.select()>0) {
					cachedThreadPool
					.invokeAll(
							selector
							.selectedKeys()
							.stream()
							.map(selectionKey->(Callable<Void>)(()->process(selectionKey)))
							.collect(Collectors.toList()));
					selector.selectedKeys().clear();
				}
			} catch (InterruptedException | IOException e) {
				e.printStackTrace();
			}
		}
	}

	private Void process(SelectionKey key) throws IOException {
		if (SelectionKey.OP_ACCEPT == (key.interestOps() & SelectionKey.OP_ACCEPT))
			if (key.isAcceptable())
				accept(key);
		if (SelectionKey.OP_READ == (key.interestOps() & SelectionKey.OP_READ))
			if (key.isReadable())
				((GridEndpoint)key.attachment()).read();
		if (SelectionKey.OP_WRITE == (key.interestOps() & SelectionKey.OP_WRITE))
			if (key.isWritable())
				((GridEndpoint)key.attachment()).write();
		return null;
	}


	private void accept(SelectionKey key) throws IOException {
		ServerSocketChannel serverChannel = (ServerSocketChannel)key.channel();
		SocketChannel channel;
		if (null != (channel = serverChannel.accept())) {
			channel.configureBlocking(false);
			channel.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, new GridEndpoint(key));
		}
	}


	public void close() {
		closed = true;
	}

	private static interface Writeable {
	}

	private static interface Readable {
	}
	
	private class GridEndpoint {

		private SelectionKey key;
		private Queue<Writeable> outputQueue = new LinkedList<Writeable>();
		private Queue<Readable> classLoaderQueue = new LinkedList<Readable>();
		private HashMap<UUID,Object> activeObjects = new HashMap<UUID, Object>();
		private ClassLoader remoteCachingClassLoader;
		
		public GridEndpoint(SelectionKey key) {
			super();
			this.key = key;
			
			// stage the channel challenge before anything else
			ServiceBootStrap challenge = new ServiceBootstrap();
			outputQueue.add(challenge);
			activeCommands.add(challenge);
		}
		
		

		public void write() {
			// writes are basically just an output queue
		}

		public void read() {
			/* 
			 * every read will be one of:
			 * 	a) create an object 
			 *  b) delete an object 
			 *  c) invoke a method on an active object (always happens on the cachedThreadPool)
			 *  d) response to an outstanding class loader invocation (typically, completes a waiting future)
			 */
		}

	}

}
