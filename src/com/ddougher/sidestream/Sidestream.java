package com.ddougher.sidestream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/** 
 * Basically a class for ephemeral, source originated, stateful sessions. Core design pattern is as follows:
 * <p>
 * A client creates a sidestream in order to have a long and stateful exchange with a server process.
 * <p>
 * Steps
 * <ul>
 * 	<li>Client creates a sidestream object</li>
 *  <li>Client makes a server call with sidestream as an argument</li>
 *  <ul>
 *  	<li>Client makes several subsequent and stateful calls over sidestream (remoting)</li>
 *  	<li>Client and server end their exchange</li>
 *  </ul>
 * </ul>
 * <p>
 * Using this pattern allows stateful requests to be channeled *around* a standard load balancer.
 * <p>
 * Channeling *through* a standard load balancer requires a straight tcp load balancer with a long timeout.
 * Such a scenario is more reminiscent of multi-call remoting and not related to this class.
 * <p>
 * 
 * @author Dana
 *
 */
public class Sidestream implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public boolean isClientSide() {
		throw new NotImplementedException();
	}
	
	public InputStream getClientInputStream() {
		throw new NotImplementedException();
	}
	
	public InputStream getServerInputStream() {
		throw new NotImplementedException();
	}
	
	public OutputStream getClientOutputStream() {
		throw new NotImplementedException();
	}

	public OutputStream getServerOuputStream() {
		throw new NotImplementedException();
	}
	
	/**
	 * when writing, convert to a waiting socket and generate a url for it
	 * @param out
	 * @throws IOException
	 */
	private void writeObject(java.io.ObjectOutputStream out)
			throws IOException
	{
		out.defaultWriteObject();
	}

	/**
	 * when reading, read the url, and connect
	 * @param in
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void readObject(java.io.ObjectInputStream in)
			throws IOException, ClassNotFoundException
	{
		in.defaultReadObject();
	}

}
