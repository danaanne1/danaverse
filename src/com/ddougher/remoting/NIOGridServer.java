package com.ddougher.remoting;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ddougher.remoting.GridProtocol.CreateObjectRequest;
import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
import com.ddougher.remoting.GridProtocol.FindClassResponse;
import com.theunknowablebits.proxamic.TimeBasedUUIDGenerator;

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
public class NIOGridServer {

	ServerSocketChannel serverChannel;
	private Selector selector;
	private ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	private volatile boolean closed = false;
	
	private class Context {
		GridProxiedClassLoader gpcl;
		Map<UUID,Object> localObjects = new HashMap<UUID, Object>();
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
			}
		}
	}
	
	// Roll in bytes until we can respond
	void read(SelectionKey key, SocketChannel channel, Context context) {
		
	}
	
	// Roll out bytes from the outgoing queue. If the queue is empty, change interest to readOnly
	void write(SelectionKey key, SocketChannel channel, Context context) {
		
	}
	

	void respond(Context context) throws IOException {
		try (
				InputStream cin = Channels.newInputStream(channel);
				BufferedInputStream bin = new BufferedInputStream(cin);
				ObjectInputStream oin = new ObjectInputStream(bin);
				OutputStream cout = Channels.newOutputStream(channel);
				BufferedOutputStream bout = new BufferedOutputStream(cout);
				ObjectOutputStream oout = new ObjectOutputStream(bout)   )
		{
			Map<UUID,Object> objects = new ConcurrentHashMap<>();
			GridProxiedClassLoader classLoader = new GridProxiedClassLoader(oout);
			while (true) {
				readIncomingRequest(oin, oout, objects, classLoader);
			}
		} finally {
			channel.close();
		}
	}

	void readIncomingRequest(ObjectInputStream in, ObjectOutputStream out, Map<UUID,Object> objects, GridProxiedClassLoader classLoader) throws IOException  {
		try {
			Object ob = in.readObject();
			cachedThreadPool.submit(
				()->
					// find and execute the function matching the type of the input object
					this
					.getClass()
					.getMethod("execute", ob.getClass(), ObjectOutputStream.class, Map.class, GridProxiedClassLoader.class)
					.invoke(this, ob, out, objects, classLoader));
		} catch (ReflectiveOperationException  e) {
			e.printStackTrace(System.err);
		}
	}
	
	void execute(CreateObjectRequest req, ObjectOutputStream out, Map<UUID, Object> objects, GridProxiedClassLoader classLoader) throws Exception  {
		UUID u = TimeBasedUUIDGenerator.instance().nextUUID();
		Object result = classLoader.loadClass(req.className).getConstructor(req.parameters).newInstance((Object [])req.args);
		objects.put(u, result);
		synchronized(out) {
			out.writeObject(new CreateObjectResponse(req.requestId,u));
			out.flush();
		}
	}

	void execute(FindClassResponse resp, ObjectOutputStream out, Map<UUID,Object> objects, GridProxiedClassLoader classLoader) throws Exception {
		classLoader.handleFindClassResponse(resp);
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