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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.ddougher.remoting.GridProtocol.CreateObjectRequest;
import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
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
public class GridServer {

	private ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	private volatile boolean closed = false;

	public GridServer(SocketAddress address) throws IOException {
		ServerSocketChannel serverChannel = ServerSocketChannel.open();
		serverChannel.bind(address);
		Thread t = new Thread(()->acceptNewIncoming(serverChannel));
		t.setDaemon(true);
		t.start();
	}

	void acceptNewIncoming(ServerSocketChannel channel) {
		if (closed) throw new IllegalStateException();
		while (!closed) {
			try {
				SocketChannel sc = channel.accept();
				new Thread(withStackDumpOnException(()->respondTo(sc))).start();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
	}

	void respondTo(SocketChannel channel) throws IOException {
		try (
				InputStream cin = Channels.newInputStream(channel);
				BufferedInputStream bin = new BufferedInputStream(cin);
				ObjectInputStream oin = new ObjectInputStream(bin);
				OutputStream cout = Channels.newOutputStream(channel);
				BufferedOutputStream bout = new BufferedOutputStream(cout);
				ObjectOutputStream oout = new ObjectOutputStream(bout)   )
		{
			Map<UUID,Object> objects = new ConcurrentHashMap<>();
			ClassLoader classLoader = null;
			while (true) {
				readIncomingRequest(oin, oout, objects, classLoader);
			}
		} finally {
			channel.close();
		}
	}

	void readIncomingRequest(ObjectInputStream in, ObjectOutputStream out, Map<UUID,Object> objects, ClassLoader classLoader) throws IOException  {
		try {
			Object ob = in.readObject();
			cachedThreadPool.submit(
				()->
					GridServer
						.this
						.getClass()
						.getMethod("execute", ob.getClass(), ObjectOutputStream.class, Map.class, ClassLoader.class)
						.invoke(this, ob, out, objects, classLoader));
		} catch (ReflectiveOperationException  e) {
			e.printStackTrace(System.err);
		}
	}
	
	void execute(CreateObjectRequest req, ObjectOutputStream out, Map<UUID, Object> objects, ClassLoader classLoader) throws Exception  {
		UUID u = TimeBasedUUIDGenerator.instance().nextUUID();
		Object result = classLoader.loadClass(req.className).getConstructor(req.parameters).newInstance(req.args);
		objects.put(u, result);
		synchronized(out) {
			out.writeObject(new CreateObjectResponse(req.requestId,u));
			out.flush();
		}
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