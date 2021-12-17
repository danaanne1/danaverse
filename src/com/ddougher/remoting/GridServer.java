package com.ddougher.remoting;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.SocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import com.ddougher.remoting.GridProtocol.CreateObjectRequest;
import com.ddougher.remoting.GridProtocol.CreateObjectResponse;
import com.ddougher.remoting.GridProtocol.FindClassResponse;
import com.ddougher.remoting.GridProtocol.RemoteInvocationRequest;
import com.ddougher.remoting.GridProtocol.RemoteInvocationResponse;
import com.theunknowablebits.proxamic.TimeBasedUUIDGenerator;

/**
 * @author Dana
 */
public class GridServer {

	private ExecutorService cachedThreadPool = SharedResources.cachedThreadPool;
	
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

	public GridServer(SocketAddress address) throws IOException {
		ServerSocketChannel serverChannel = ServerSocketChannel.open();
		serverChannel.bind(address);
		Thread t = new Thread(()->acceptNewIncoming(serverChannel));
		t.setDaemon(true);
		t.start();
	}

	void acceptNewIncoming(ServerSocketChannel channel) {
		while (true) {
			try {
				new Context(channel.accept()).start();
			} catch (IOException e) {
				e.printStackTrace(System.err);
			}
		}
	}

	private class Context {
		boolean done;
		SocketChannel channel;
		ObjectInputStream oin;
		ObjectOutputStream oout;
		ConcurrentHashMap<UUID, Object> instances;
		GridProxiedClassLoader classLoader;
		
		public Context(SocketChannel channel) {
			super();
			this.channel = channel;
		}
		public void start() {
			new Thread(withStackDumpOnException(this::run)).start();
		}
		public void run() throws IOException {
			try (	InputStream cin = Channels.newInputStream(channel);
					BufferedInputStream bin = new BufferedInputStream(cin);
					ObjectInputStream oin = new ObjectInputStream(bin);
					OutputStream cout = Channels.newOutputStream(channel);
					BufferedOutputStream bout = new BufferedOutputStream(cout);
					ObjectOutputStream oout = new ObjectOutputStream(bout); )
			{
				this.done = false;
				this.oin = oin;
				this.oout = oout;
				this.instances = new ConcurrentHashMap<>();
				this.classLoader = new GridProxiedClassLoader(oout);
				while (!done) 
					readAndDispatchIncoming();
			} finally {
				channel.close();
			}

		}

		void readAndDispatchIncoming() throws IOException  {
			try {
				Object ob = oin.readObject();
				cachedThreadPool.submit(
					()->
						// find and execute the function matching the type of the input object
						this
						.getClass()
						.getMethod("execute", ob.getClass())
						.invoke(this, ob));
			} catch (ReflectiveOperationException  e) {
				e.printStackTrace(System.err);
			}
		}
		
		
		@SuppressWarnings("unused")
		void execute(CreateObjectRequest req) throws Exception  {
			UUID u = TimeBasedUUIDGenerator.instance().nextUUID();
			Object result = classLoader.loadClass(req.className).getConstructor(req.parameters).newInstance((Object [])req.args);
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
			Serializable result = (Serializable)target.getClass().getMethod(req.methodName, req.parameters).invoke(target, (Object [])req.args);
			synchronized(oout) {
				oout.writeObject(new RemoteInvocationResponse(req.requestId, req.objectId, result));
				oout.flush();
			}
		}
	}
	

}