package com.ddougher.remoting;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import com.ddougher.remoting.GridProtocol.FindClassRequest;
import com.ddougher.remoting.GridProtocol.FindClassResponse;

public class GridProxiedClassLoader extends ClassLoader {

	private ObjectOutputStream out;
	private ConcurrentHashMap<String, CompletableFuture<byte []>> pendingMap = new ConcurrentHashMap<>();
	private static final ConcurrentHashMap<String, Class<?>> builtins = new ConcurrentHashMap<>();
	
	
	static {
		builtins.put(int.class.getName(), int.class);
		builtins.put(long.class.getName(), long.class);
		builtins.put(float.class.getName(), float.class);
		builtins.put(double.class.getName(), double.class);
		builtins.put(byte.class.getName(), byte.class);
		builtins.put(char.class.getName(), char.class);
		builtins.put(boolean.class.getName(), boolean.class);
	}
	
	// see https://stackoverflow.com/questions/35071016/how-to-get-bytecode-as-byte-array-from-class 
	// for details of how the other end can work
	
	public GridProxiedClassLoader(ObjectOutputStream out) {
		super();
		this.out = out;
		registerAsParallelCapable();
	}
	
	public GridProxiedClassLoader(ClassLoader parent, ObjectOutputStream out) {
		super(parent);
		this.out = out;
		registerAsParallelCapable();
	}

	@Override
	protected Class<?> findClass(String name) throws ClassNotFoundException {
		Class<?> c = builtins.get(name);
		if(c != null) return c;
		try {
			CompletableFuture<byte []> c1,c2;
			if (null==(c2=pendingMap.putIfAbsent(name, c1 = new CompletableFuture<byte[]>()))) {
				synchronized(out) {
					out.writeObject(new FindClassRequest(name));
					out.flush();
				}
				c2=c1;
			} 
			byte [] b = c2.get();
			return defineClass(name, b, 0, b.length);
		} catch (IOException | InterruptedException | ExecutionException  e) {
			throw new ClassNotFoundException("Unable to load class because",e);
		}
	}

	public void handleFindClassResponse(FindClassResponse resp) {
		pendingMap.remove(resp.className).complete(resp.bytes);
	}
	
}
