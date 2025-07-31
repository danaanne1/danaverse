package com.ddougher.remoting;

import java.io.*;
import java.lang.reflect.Proxy;
import java.net.*;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import com.ddougher.documentstore.MemoryMappedDocumentStore;
import com.ddougher.remoting.GridProtocol.CreateObjectRequest;
import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
import com.ddougher.remoting.GridProtocol.FindClassResponse;
import com.ddougher.remoting.GridProtocol.RemoteInvocationRequest;
import com.ddougher.remoting.GridProtocol.RemoteInvocationResponse;

/**
 * @author Dana
 */
public class GridServer {

	private volatile boolean shutdown = false;
	private transient ServerSocket serverSocket;
	private boolean debug = false;
	
	public GridServer(SocketAddress address) throws IOException, ClassNotFoundException {
		GridContext.context.put("DanaMarketData", initDocStore());
		this.serverSocket = new ServerSocket();
		serverSocket.bind(address);
	}

	public static void main(String [] args) throws IOException, ClassNotFoundException {
		System.out.println("Starting Grid Server on port " + Integer.parseInt(args[0]) );
		System.out.println("Available Processors " + Runtime.getRuntime().availableProcessors());
		System.out.println("Max Memory " + Runtime.getRuntime().maxMemory());
		InetSocketAddress serverAddress = new InetSocketAddress(Integer.parseInt(args[0]));
		GridServer server = new GridServer(serverAddress);
		server.start();
	}

	public MemoryMappedDocumentStore initDocStore() throws IOException, ClassNotFoundException {
		final File file = new File("DanaStockData", "Database.dt1");
		file.getParentFile().mkdirs();
		MemoryMappedDocumentStore docStore;
		if (file.exists()) {
			try (	FileInputStream fin = new FileInputStream(file);
					BufferedInputStream bin = new BufferedInputStream(fin, 65536);
					ObjectInputStream ois = new ObjectInputStream(bin))
			{
				docStore = (MemoryMappedDocumentStore)ois.readObject();
				System.out.println("Database loaded from file " + file.getAbsolutePath());
			}
		} else {
			docStore = new MemoryMappedDocumentStore(
					Optional.of("DanaStockData"),
					Optional.empty(),
					Optional.empty(),
					Optional.empty(),
					Optional.empty()
			);
			System.out.println("Database created");
		}
		final MemoryMappedDocumentStore it = docStore;
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try (FileOutputStream fout = new FileOutputStream(file);
				 BufferedOutputStream bout = new BufferedOutputStream(fout, 65536);
				 ObjectOutputStream oos = new ObjectOutputStream(bout)) {
				it.close();
				System.out.println("Database closed");
				oos.writeObject(it);
				System.out.println("Database written to file " + file.getAbsolutePath());
				oos.flush();
			} catch (IOException ex) {
				ex.printStackTrace(System.out);
			}
		}));
		return it;
	}


	public void start() {
		Thread t = new Thread(()->acceptNewIncoming());
		t.setDaemon(false);
		t.start();
	}
	
	public SocketAddress getBoundAddress() throws IOException {
		return serverSocket.getLocalSocketAddress();
	}
	
	public void shutdown() {
		shutdown = true;
	}

	void acceptNewIncoming() {
		while (!shutdown) {
			try {
				new ServerContext(serverSocket.accept()).start();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
	}

	private class ServerContext {
		boolean done;
		Socket socket;
		ObjectInputStream oin;
		ObjectOutputStream oout;
		ConcurrentHashMap<UUID, Object> instances;
		GridProxiedClassLoader classLoader;
		
		public ServerContext(Socket socket) {
			super();
			this.socket = socket;
		}
		public void start() {
			new Thread(SharedResources.withStackDumpOnException(this::run)).start();
		}
		public void run() throws IOException {
			try (  	InputStream cin = socket.getInputStream();
					BufferedInputStream bin = new BufferedInputStream(cin);
					ObjectInputStream oin = new ObjectInputStream(bin); ) {
				try (OutputStream cout = socket.getOutputStream();
					 BufferedOutputStream bout = new BufferedOutputStream(cout);
					 ObjectOutputStream oout = new ObjectOutputStream(bout);) {
					oout.flush(); // force out the stream headers
					this.done = false;
					this.oin = oin;
					this.oout = oout;
					this.instances = new ConcurrentHashMap<>();
					this.classLoader = new GridProxiedClassLoader(getClass().getClassLoader(), oout);
					while (!done && !shutdown)
						readAndDispatchIncoming();
				}
			} catch (SocketException | EOFException broke) {
				System.out.println(broke.getMessage());
			} finally {
				socket.close();
			}
		}

		void readAndDispatchIncoming() throws IOException  {
			try 
			{
				Object ob = oin.readObject();
				if (ob instanceof GridProtocol.CloseRequest) {
					done = true;
					synchronized(oout) {
						oout.writeObject(new GridProtocol.CloseResponse());
						oout.flush();
					}
					return;
				}
				SharedResources.cachedThreadPool.execute(
					SharedResources.withStackDumpOnException(()-> {
						// find and execute the function matching the type of the input object
						Object o = upConvert(ob);
						if (debug) System.out.println(this.toString() + ":" + o);
						this
								.getClass()
								.getDeclaredMethod("execute", o.getClass())
								.invoke(this, o);
					}
				));
			} catch (ReflectiveOperationException  e) {
				e.printStackTrace(System.err);
			}
		}
		
		/*
		 * upConverting allows us to piggyback the deserialization classloader on the same stream we are loading classes from.
		 */
		private Object upConvert(Object ob) throws IOException, ClassNotFoundException {
			if (ob instanceof byte[]) {
				try (ByteArrayInputStream bin = new ByteArrayInputStream((byte [])ob);
						ObjectInputStream ooin = new ObjectInputStream(bin){
							protected Class<?> resolveClass(java.io.ObjectStreamClass desc) throws IOException , ClassNotFoundException {
								try {
									return Class.forName(desc.getName(), true, classLoader);
								} catch (Exception e) {
									return super.resolveClass(desc);
								}
							}
							protected Class<?> resolveProxyClass(String[] interfaces) throws IOException ,ClassNotFoundException {
								try {
									Class<?> [] interfaceClasses = (Class[]) Arrays.stream(interfaces).map(this::resolve).toArray();
									return Proxy.getProxyClass(classLoader, interfaceClasses);
								} catch (Exception e) {
									return super.resolveProxyClass(interfaces);
								}
							};
							private Class<?> resolve(String interfaceName) {
								try {
									return Class.forName(interfaceName, true, classLoader);
								} catch (Exception re) {
									if (re instanceof RuntimeException) throw (RuntimeException)re;
									throw new RuntimeException(re);
								}
							}
						}) 
				{
					ob = ooin.readObject();
				}
			}
			return ob;
		}
		
		@SuppressWarnings("unused")
		void execute(CreateObjectRequest req) throws Exception  {
			UUID u = TimeBasedUUIDGenerator.instance().nextUUID();
			Object result = req.objectClass.getConstructor(req.parameters).newInstance((Object [])req.args);
			instances.put(u, result);
			synchronized(oout) {
				oout.writeObject(new CreateObjectResponse(req.requestId,u));
				oout.flush();
			}
		}

		@SuppressWarnings("unused")
		void execute(FindClassResponse resp) throws Exception {
			classLoader.handleFindClassResponse(resp);
		}
		
		@SuppressWarnings("unused")
		void execute(RemoteInvocationRequest req) throws Exception {
			Object target = instances.get(req.objectId);
			Object result = target.getClass().getMethod(req.methodName, req.parameterTypes).invoke(target, req.args);
			synchronized(oout) {
				oout.writeObject(new RemoteInvocationResponse(req.requestId, result));
				oout.flush();
			}
		}
	}
	

}