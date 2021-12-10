package com.ddougher.remoting;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.ddougher.remoting.GridProtocol.FindClassRequest;
import com.ddougher.remoting.GridProtocol.FindClassResponse;

public class GridProxiedClassLoader extends ClassLoader {

	private ObjectOutputStream out;
	private ConcurrentHashMap<String, CompletableFuture<byte []>> pendingMap = new ConcurrentHashMap<>();
	
	// see https://stackoverflow.com/questions/35071016/how-to-get-bytecode-as-byte-array-from-class 
	// for details of how the other end can work
	
	public GridProxiedClassLoader(ObjectOutputStream out) {
		super();
		this.out = out;
		registerAsParallelCapable();
	}
	
	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		try {
			CompletableFuture<byte []> c1,c2;
			if (null==(c2=pendingMap.putIfAbsent(name, c1 = new CompletableFuture<byte[]>()))) {
				synchronized(out) {
					out.writeObject(new FindClassRequest(name));
				}
				c2=c1;
			} 
			byte [] b = c2.get(30, TimeUnit.SECONDS);
			return defineClass(name, b, 0, b.length);
		} catch (IOException | InterruptedException | ExecutionException | TimeoutException e) {
			throw new ClassNotFoundException("Unable to load class because",e);
		}
	}

	public void handleFindClassResponse(FindClassResponse resp) {
		pendingMap.remove(resp.className).complete(resp.bytes);
	}
	
}
