package com.ddougher.sidestream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;


import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/** 
 * A Sidestream is a stream tunneling class that is serializable.
 * <p>
 * With sidestream, you can wrap any standard stream with a serializable tunnel, ie:
 * 
 * <code>
 *  // Client Side:
 * 	FileOutputStream fout = new FileOutputStream("logfile");
 *  OutputSideStream sout = new OutputSideStream(fout);
 *  Server.doSomeRemoteThing(sout);
 *  
 *  // Server Side:
 *  public void doSomeRemoteThing(OutputSideStream out) {
 *  	PrintStream pout = new PrintStream(out);
 *  	pout.println("this is a log entry"); // writes output to the log file on client
 *  }
 * </code>
 * 
 * When a side stream is serialized out, it establishes a listening socket. When serialized in,
 * that socket is connected to on the in side. 
 * <p>
 * Naturally, this does not work for long term serialization plays. Or with multiple remotes. 
 * <p>
 * Sidestreams will also work locally without serialization.
 * <p>
 * Clearly, sidestreams require network connectivity between a and b. Serialization to disparate networks
 * will not work. This is mainly a convenience for local in VPN behaviors, since any robust network protocol 
 * requires additional treatment.
 * 
 * @author Dana
 *
 */
public class OutputSidestream extends OutputStream implements Serializable {

	ServerSocket serverSocket = null;
	OutputStream delegate = null;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public OutputSidestream(OutputStream source) {
		delegate = source;
	}
	
	protected OutputSidestream() {
		
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
		
		// TODO: Thread Based Inefficient Naive Version. Replace with delimited byte arrays. Replace with NIO
		serverSocket = new ServerSocket();
		serverSocket.bind(null);
		out.writeObject(serverSocket.getInetAddress());
		out.writeObject(serverSocket.getLocalPort());
		Thread t = new Thread(() -> {
			try {
				Socket s = serverSocket.accept();
				InputStream in = s.getInputStream();
				for (int i = in.read(); i != -1; i=in.read())
					delegate.write(i);
				s.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
			try {
				delegate.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		t.start();
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
		// connect to the listening socket
	}

	
	public static void main(String [] args) throws UnknownHostException {
		System.out.println(InetAddress.getLocalHost().getHostAddress());
	}

	@Override
	public void write(int b) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}
