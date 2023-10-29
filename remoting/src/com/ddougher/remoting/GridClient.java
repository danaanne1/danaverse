package com.ddougher.remoting;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.instrument.UnmodifiableClassException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import com.ddougher.remoting.GridProtocol.CreateObjectRequest;
import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
import com.ddougher.remoting.GridProtocol.FindClassRequest;
import com.ddougher.remoting.GridProtocol.FindClassResponse;
import com.ddougher.remoting.GridProtocol.RemoteInvocationRequest;
import com.ddougher.remoting.GridProtocol.RemoteInvocationResponse;


/** A client for a grid server */
public class GridClient implements Closeable {


	SocketAddress address;
	transient ObjectInputStream oin;
	transient ObjectOutputStream oout;
	transient boolean closed = false;
	transient Thread thread;
	transient boolean debug = true;
	
	public GridClient(SocketAddress address) {
		this.address = address;
	}

	void start() {
		synchronized (this) {
			(thread = new Thread(SharedResources.withStackDumpOnException(this::run))).start();
			try {
				this.wait(); // wait for thread to achieve full startup
			} catch (InterruptedException e) {
			}
		}
	}

	void run() throws IOException {
		try (Socket socket = new Socket() )
		{
			socket.connect(address);
			try (	OutputStream cout = socket.getOutputStream();
					BufferedOutputStream bout = new BufferedOutputStream(cout);
					ObjectOutputStream oout = new ObjectOutputStream(bout); ) 
			{
				oout.flush(); // force out the stream headers
				try (	InputStream cin = socket.getInputStream();
						BufferedInputStream bin = new BufferedInputStream(cin);
						ObjectInputStream oin = new ObjectInputStream(bin); )
				{
					this.oin = oin;
					this.oout = oout;
					synchronized (this) {
						this.notify();
					}
					while(!closed) {
						handleIncoming();
					}
				}
			}
		}
	}

	void handleIncoming() throws IOException  {
		try {
			Object ob = oin.readObject();
			SharedResources.cachedThreadPool.execute(
				SharedResources.withStackDumpOnException(() -> {
					if (debug) System.out.println(this.toString() + ":" +ob);
					// find and execute the function matching the type of the input object
					this
						.getClass()
						.getMethod("execute", ob.getClass())
						.invoke(this, ob);
				}));
		} catch (ReflectiveOperationException  e) {
			e.printStackTrace(System.err);
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
	

	@Override
	public void close() throws IOException {
		closed = true;
		thread.interrupt();
	}

	/**
	 * a cache of FindClassResponse Futures.
	 * 
	 *  
	 *  
	 */
	static final HashMap<String, CompletableFuture<FindClassResponse>> classLoaderCache = new HashMap<String, CompletableFuture<FindClassResponse>>();
	/**
	 * Received from the remote (server) when it needs to load a class.
	 * @param req
	 * @param out
	 * @param objects
	 * @throws Exception
	 */
	public void execute(FindClassRequest req) throws Exception {

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

		synchronized (oout) {
			oout.writeObject(response);
			oout.flush();
		}
	}

	/*
	 * This section supports request awaiting
	 */
	transient ConcurrentHashMap<UUID, CompletableFuture<Object>> awaitedRequests = new ConcurrentHashMap<>();
	@SuppressWarnings("unchecked")
	<T> T awaitRequest(UUID reqID, Object toWrite) throws IOException, InterruptedException, ExecutionException {
		CompletableFuture<Object> future = new CompletableFuture<Object>();
		awaitedRequests.put(reqID, future);
		synchronized (oout) {
			oout.writeObject(toWrite);
			oout.flush();
		}
		Object ob = future.get();
		awaitedRequests.remove(reqID);
		return (T)ob;
	}
	
	/*
	 * this section allows us to create remote objects and returns a local proxy that transparently tunnels api to the remote.
	 */
	transient Map<Object,UUID> IDByProxy = Collections.synchronizedMap(new WeakHashMap<>());
	@SuppressWarnings("unchecked")
	public <T> T createRemoteObject(Class<T> interfaceClass, Class<?> implClass, Class<?> [] params, Object [] args) throws IOException, InterruptedException, ExecutionException {
		UUID requestID = SharedResources.UUIDGenerator.nextUUID();
		Object toWrite = downConvert(new CreateObjectRequest(requestID, implClass, params, args));
		UUID objectID = awaitRequest(requestID, toWrite);
		T t = (T)Proxy.newProxyInstance(ClassLoader.getSystemClassLoader(), new Class<?> [] { interfaceClass }, (p,m,a)->invoke(p,m,a,interfaceClass));
		IDByProxy.put(t, objectID);
		return t;
	}
	public void execute(CreateObjectResponse res) throws Exception  {
		awaitedRequests.get(res.requestId).complete(res.objectId);
	}

	
	/*
	 * this section implements the remote invocation handler used by our remote object proxies
	 */
	public Object invoke(Object proxy, Method method, Object[] args, Class<?> interfaceClass) throws Throwable {
		if (method.isDefault())
			return handleDefaultMethod(proxy, method, args, interfaceClass);
		if (method.getName()=="hashCode") 
			return System.identityHashCode(proxy);
		if (method.getName()=="equals") 
			return proxy==args[0];
		UUID reqID = SharedResources.UUIDGenerator.nextUUID();
		UUID objectID = IDByProxy.get(proxy);
		Object toWrite = downConvert(new RemoteInvocationRequest(reqID, objectID, method.getName(), method.getParameterTypes(), args));
		return awaitRequest(reqID, toWrite);
	}
	private static final Object handleDefaultMethod(Object proxy, Method method, Object [] args, Class<?> interfaceClass) throws Throwable {
		// this can get very ugly between Java8 and Java 9+
		// for more detail see this
		// https://blog.jooq.org/2018/03/28/correct-reflective-access-to-interface-default-methods-in-java-8-9-10/
		Lookup lookup;
		try {
			Constructor<Lookup> constructor = Lookup.class.getDeclaredConstructor(Class.class);
			constructor.setAccessible(true);
			lookup = constructor.newInstance(interfaceClass);
		} catch (Exception e) {
			lookup = MethodHandles.lookup();
		}
		return 
			lookup
				.findSpecial(interfaceClass, method.getName(), MethodType.methodType(method.getReturnType(), method.getParameterTypes()), interfaceClass)
				.bindTo(proxy)
				.invokeWithArguments(args);
	}
	public void execute(RemoteInvocationResponse res) throws Exception {
		awaitedRequests.get(res.requestId).complete(res.result);
	}

	/*
	 * This downconverts objects for serialization. 
	 * This is neccessary so that the remote can piggyback classloader requests on the same stream that the object is read from.
	 */
	private byte [] downConvert(Object ob) throws IOException {
		ByteArrayOutputStream bout = new ByteArrayOutputStream();
		ObjectOutputStream obout = new ObjectOutputStream(bout);
		obout.writeObject(ob);
		obout.flush();
		obout.close();
		bout.close();
		return bout.toByteArray();
	}

}
