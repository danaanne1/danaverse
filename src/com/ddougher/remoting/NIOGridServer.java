package com.ddougher.remoting;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ddougher.remoting.GridProtocol.CreateObjectRequest;
import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
import com.ddougher.remoting.GridProtocol.FindClassResponse;
import com.theunknowablebits.proxamic.TimeBasedUUIDGenerator;

/**
 * NIO Remote Object Server with ClassLoading capability.
 * 
 * @author Dana
 */
public class NIOGridServer {

	private ServerSocketChannel serverChannel;
	private Selector selector;
	private ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	private volatile boolean closed = false;
	private ConcurrentLinkedDeque<ByteBuffer> bufferPool = new ConcurrentLinkedDeque<ByteBuffer>();
	
	
	private class Context {
		GridProxiedClassLoader gpcl;
		Map<UUID,Object> instances = new HashMap<UUID, Object>();
		LinkedList<ByteBuffer> incoming = new LinkedList<>(Collections.singleton(ByteBuffer.allocateDirect(65535)));
		LinkedList<ByteBuffer> outgoing = new LinkedList<>();
		ClassLoader classloader;
	}

	public NIOGridServer(SocketAddress address) throws IOException {
		selector = Selector.open();
		serverChannel = ServerSocketChannel.open();
		serverChannel.bind(address);
		serverChannel.configureBlocking(false);
		serverChannel.register(selector, SelectionKey.OP_ACCEPT);
		Thread t = new Thread(this::incomingMultiplex);
		t.setDaemon(true);
		t.start();
	}

	void incomingMultiplex() {
		while (!closed) {
			try {
				selector.select();
				for (SelectionKey key: selector.selectedKeys()) {
					if (key.channel() == serverChannel && key.isAcceptable()) {
						SocketChannel sc;
						while (null!=(sc=serverChannel.accept())) {
							sc.configureBlocking(false);
							sc.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, new Context());
						}
					} else if (key.channel() instanceof SocketChannel) {
						if (key.isReadable())
							read(key, (SocketChannel)key.channel(), (Context)key.attachment());
						if (key.isWritable())
							write(key, (SocketChannel)key.channel(), (Context)key.attachment());
					}
						
				}
			} catch (IOException ioe) {
				ioe.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	// Roll in bytes until we can respond
	void read(SelectionKey key, SocketChannel channel, Context context) throws IOException, ClassNotFoundException {
		ByteBuffer buffer;
		int c=1;
		while (c>0) {
			if (context.incoming.isEmpty() || !context.incoming.getLast().hasRemaining()) 
				context.incoming.add(allocateBuffer());
			c = channel.read(context.incoming.getLast());
			decantReadyMessage(context);
		}
	}
	
	ByteBuffer allocateBuffer() {
		ByteBuffer buffer;
		if (null==(buffer=bufferPool.poll())) buffer = ByteBuffer.allocateDirect(65535);
		return buffer;
	}
	
	void decantReadyMessage(Context context) throws IOException, ClassNotFoundException {
		long totalLength = context.incoming.stream().reduce(0L,(val,buf)->val+buf.position(),(a,b)->a+b);
		Object result;
		if (totalLength > 8) {
			long messageLength = context.incoming.getFirst().getLong(0);
			if (totalLength >= messageLength) {
				try (	InputStream gin = (InputStream)new DecantingInputStream(messageLength,context.incoming);
						ObjectInputStream oin = new ObjectInputStream(gin); ) 
				{
					result = oin.readObject();
				}
				long remainingLength = messageLength;
				while (remainingLength > 0) {
					ByteBuffer buffer = context.incoming.peekFirst();
					if (buffer.position()<remainingLength) {
						remainingLength -= buffer.position();
						bufferPool.add((ByteBuffer)context.incoming.poll().clear());
						continue;
					} 
					buffer.flip();
					buffer.position((int)remainingLength);
					buffer.compact();
					if (buffer.position()<=0) 
						bufferPool.add((ByteBuffer)context.incoming.poll().clear());
					remainingLength = 0;
				}
				dispatchMessage(context, result);
			}
		}
	}

	private void dispatchMessage(Context context, Object result) {
		cachedThreadPool.submit(
			()->
				// find and execute the function matching the type of the input object
				this
				.getClass()
				.getMethod("execute", result.getClass(), Context.class)
				.invoke(this, result, context));
	}
	
	private class DecantingInputStream extends InputStream {

		long messageLength;
		LinkedList<ByteBuffer> incoming;

		public DecantingInputStream(long messageLength, LinkedList<ByteBuffer> incoming) {
			super();
			this.messageLength = messageLength;
			this.incoming = incoming;
		}

		@Override
		public int read() throws IOException {
			byte [] b = new byte[1];
			read(b);
			return b[0];
		}
		
		@Override
		public int read(byte[] b) throws IOException {
			return read(b,0,b.length);
		}
		
		@Override
		public int read(byte[] b, int off, int len) throws IOException {

			return 0;
		}
	}
	
	
	// Roll out bytes from the outgoing queue. If the queue is empty, change interest to readOnly
	void write(SelectionKey key, SocketChannel channel, Context context) {
		
	}
	
	void execute(CreateObjectRequest req, Context context) throws Exception  {
		UUID u = TimeBasedUUIDGenerator.instance().nextUUID();
		Object result = context.classloader.loadClass(req.className).getConstructor(req.parameters).newInstance((Object [])req.args);
		context.instances.put(u, result);
		writeToContext(new CreateObjectResponse(req.requestId,u));
	}

	void execute(FindClassResponse resp, Context context) throws Exception {
		context.classloader.handleFindClassResponse(resp);
	}

	interface Invocation {
		public void run() throws Exception; 
	}
	
	Runnable withStackDumpOnException(Invocation i) {
		return () -> {
			try {
				i.run();
			} catch (Exception e) {
				e.printStackTrace(System.err);
			}
		};
	
	}

}