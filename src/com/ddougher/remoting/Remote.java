package com.ddougher.remoting;

import java.io.Closeable;
import java.io.InputStream;
import java.io.OutputStream;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * A class for tunneling service calls to a remote session.
 * <p>
 * Eventually this will include support for remote class loading.
 * 
 * @author Dana
 *
 */
public class Remote {
	
	// TODO fix this to use NIO!
	
	
	public static <T extends Closeable> T client(Class<T> serviceApi, Class<? extends T> implClass, InputStream fromServer, OutputStream toServer ) {

		// typical round trips
		// client invocation
		// 		server class loading requests can happen at any time (possibly batched up during an inflight class loading request)
		// 		client result
		
		throw new NotImplementedException();
	}

	public static void serve(String interfaceClassName, String implClassName, InputStream fromClient, OutputStream toClient ) {
		
		// create a classloader to proxy over the channel
		// load implClass and serviceApi using the proxy classloader
		// listen for method invocations and tunnel them to implClass

		throw new NotImplementedException();
	}
	
	
}
