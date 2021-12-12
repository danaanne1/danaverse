package com.ddougher.remoting;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.instrument.UnmodifiableClassException;
import java.net.SocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
import com.ddougher.remoting.GridProtocol.FindClassRequest;
import com.ddougher.remoting.GridProtocol.FindClassResponse;


/** A client for a grid server */
public class GridClient implements Closeable {

	public static final HashMap<String, CompletableFuture<FindClassResponse>> classLoaderCache = new HashMap<String, CompletableFuture<FindClassResponse>>();

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
			SharedResources.cachedThreadPool.submit(
				()-> // find and execute the function matching the type of the input object
					this
					.getClass()
					.getMethod("execute", ob.getClass(), ObjectOutputStream.class, Map.class)
					.invoke(this, ob, out, objects));
		} catch (ReflectiveOperationException  e) {
			e.printStackTrace(System.err);
		}
	}
	
	/** 
	 * Received from the remote (server) in response to a CreateObjectRequest
	 * @param res
	 * @param out
	 * @param objects
	 * @throws Exception
	 */
	void execute(CreateObjectResponse res, ObjectOutputStream out, Map<UUID, Object> objects) throws Exception  {

		
	}

	/**
	 * Received from the remote (server) when it needs to load a class.
	 * @param req
	 * @param out
	 * @param objects
	 * @throws Exception
	 */
	void execute(FindClassRequest req, ObjectOutputStream out, Map<UUID, Object> objects) throws Exception {

		CompletableFuture<FindClassResponse> futureResponse, mine = null;
		synchronized (classLoaderCache) {
			if (!classLoaderCache.containsKey(req.className)) 
				classLoaderCache.put(req.className, mine = futureResponse = new CompletableFuture<FindClassResponse>());
			else
				futureResponse = classLoaderCache.get(req.className);
		}

		if (null!=mine) 
			mine.complete(new FindClassResponse(req.className, readClassBytes(req.className)));

		FindClassResponse response = futureResponse.get();

		synchronized (out) {
			out.writeObject(response);
			out.flush();
		}
	}
	
	private byte [] readClassBytes(String className) throws IOException, ClassNotFoundException, UnmodifiableClassException, InterruptedException, ExecutionException {
		try {
			return ByteCodeExtractor.getClassBytes(Class.forName(className));
		} catch (IllegalStateException ISE) {
			// as a fallback, attempt to load from a classfile:
			InputStream in = ClassLoader.getSystemResourceAsStream(className.replace('.', '/').concat(".class"));
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
			b = new byte[d];
			d = 0;
			for (byte [] bb: bytes) {
				System.arraycopy(bb, 0, b, d, bb.length);
				d+=bb.length;
			}
			return b;
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
