package com.ddougher.remoting;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.ddougher.remoting.GridProtocol.CreateObjectRequest;
import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
import com.ddougher.remoting.GridProtocol.FindClassRequest;
import com.ddougher.remoting.GridProtocol.FindClassResponse;
import com.theunknowablebits.proxamic.TimeBasedUUIDGenerator;


/** A client for a grid server */
public class GridClient implements Closeable {

	private ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

	public GridClient(SocketAddress address) throws IOException {
		SocketChannel serverChannel = SocketChannel.open();
		serverChannel.configureBlocking(false);
		
		new Thread(withStackDumpOnException(()->respondTo(serverChannel))).start();
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
			while (true) {
				// sendOutgoingRequest(oin, oout, objects);
				handleIncoming(oin, oout, objects);
			}
		} finally {
			channel.close();
		}
	}

	void handleIncoming(ObjectInputStream in, ObjectOutputStream out, Map<UUID,Object> objects) throws IOException  {
		try {
			Object ob = in.readObject();
			cachedThreadPool.submit(
				()->
					// find and execute the function matching the type of the input object
					this
					.getClass()
					.getMethod("execute", ob.getClass(), ObjectOutputStream.class, Map.class)
					.invoke(this, ob, out, objects));
		} catch (ReflectiveOperationException  e) {
			e.printStackTrace(System.err);
		}
	}
	
	void execute(CreateObjectResponse res, ObjectOutputStream out, Map<UUID, Object> objects) throws Exception  {

		
	}

	HashMap<String, CompletableFuture<FindClassResponse>> classLoaderCache = new HashMap<String, CompletableFuture<FindClassResponse>>();
	
	void execute(FindClassRequest req, ObjectOutputStream out, Map<UUID, Object> objects) throws Exception {

		// this is ugly. needs cleaning
		CompletableFuture<FindClassResponse> result;
		boolean shouldComplete = false;
		synchronized (classLoaderCache) {
			if (!classLoaderCache.containsKey(req.className)) {
				classLoaderCache.put(req.className, result = new CompletableFuture<FindClassResponse>());
				shouldComplete = true;
			} else {
				result = classLoaderCache.get(req.className);
			}
		}
		if (shouldComplete) {
			// read the class into an array of bytes:
			InputStream in = ClassLoader.getSystemResourceAsStream(req.className.replace('.', '/'));
			LinkedList<byte []> bytes = new LinkedList<>();
			byte [] buf = new byte[16536];
			int c, d=0;
			byte [] b;
			while (-1!=(c=in.read(buf))) {
				d+=c;
				bytes.add(b=new byte[c]);
				System.arraycopy(buf, 0, b, 0, c);
			}
			// copy the read bytes to a finished array
			FindClassResponse resp = new FindClassResponse(req.className,b = new byte[d]);
			d = 0;
			for (byte [] bb: bytes) {
				System.arraycopy(bb, 0, b, d, bb.length);
				d+=bb.length;
			}
			result.complete(resp);
		}
		FindClassResponse resp = result.get();
		synchronized (out) {
			out.writeObject(resp);
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

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

}
