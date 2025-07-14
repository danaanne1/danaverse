package com.ddougher.remoting;

import java.net.NetworkInterface;
import java.security.SecureRandom;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.UUID;

/**
 * A UUID Generator that generates timestamp based UUIDS and given unique node addresses is guaranteed never to overlap
 * with another UUID from this same generator.
 * <p>
 * To reduce the possibility of overlap with other JVM's running on the same host, a random clockSequence value is used.
 * <p>
 * To guarantee the impossibility of overlap, an instance can be obtained with a declared clockSequence. This allows clock sequences
 * to be allocated from a cloud based number generator. (up to 8k distinct clock sequences can be assigned for each node address)
 * <p>
 * This does not strictly adhere to the nanosecond clock, however it will be time consistent.
 * Nanosecond values are supplied via a monoatomically increasing counter that resets each millisecond.
 * <p>
 * @author Dana
 *
 */
public class TimeBasedUUIDGenerator 
{
	private static final SecureRandom random = new SecureRandom();

	// lsb
	private static final long nodeMacAddr =  getNodeMacAddr();
	private transient long clockSeq = random.nextInt()& 0x1fffl;
	private static final long variant = 2l;

	// msb
	public static final long EPOCH_OFFSET_MILLIS = 0xb1d069b5400l;
	private static final long version = 1l;
	private long lastTimestamp=0;
	private int nanosPart=0; 

	public TimeBasedUUIDGenerator(int clockSeq) {
		this.clockSeq = clockSeq & 0x1fffl;
	}

	private static TimeBasedUUIDGenerator instance = new TimeBasedUUIDGenerator(random.nextInt()); 

	public void setInstance(TimeBasedUUIDGenerator generator) { instance = generator; }
	
	public static TimeBasedUUIDGenerator instance() { return instance; }
	
	
	public UUID nextUUID()
	{
		long lsb = 
				( nodeMacAddr    & 0x0000ffffffffffffl )
				| ( clockSeq<<48 & 0x1fff000000000000l )
				| ( variant<<62  & 0xc000000000000000l );
		long nextTimeStamp = nextTimestamp();
		long msb = 
				( nextTimeStamp>>48   & 0x0000000000000fffl )
				| ( version<<12       & 0x000000000000f000l )
				| ( nextTimeStamp>>16 & 0x00000000ffff0000l )
				| ( nextTimeStamp<<32 & 0xffffffff00000000l );
		return new UUID(msb,lsb);
	}

	// 48 bits (6 bytes)
	public static long getNodeMacAddr()  
	{
		long networkAddr = 0l;
		byte [] b = new byte[6];
		random.nextBytes(b);
		try
		{
			Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
			firstNetworkAddr:
				while (e.hasMoreElements())
				{
					NetworkInterface if1 = e.nextElement();
					if (if1.isUp()&&(!if1.isLoopback()))
					{
						b = if1.getHardwareAddress();
						break firstNetworkAddr;
					}
				}
		}
		catch (Exception e)
		{
		}
		for (int i = 0; (i < 6)&&(i < b.length); i++)
		{
			if (b[i]<0)
				networkAddr = networkAddr<<8 | ((b[i]&0x7fl)|0x80l);
			else
				networkAddr = networkAddr<<8 | (b[i]&0x7fl);
		}
		return networkAddr&0x0000ffffffffffffl;
	}

	/**
	 * This method can generate up to 10m UUIDs/second.
	 * @return
	 */
	private long nextTimestamp()
	{
		long timestamp =((System.currentTimeMillis()+EPOCH_OFFSET_MILLIS)*10000) & 0x0FFFffffFFFFffffl;
		synchronized (TimeBasedUUIDGenerator.class) 
		{
			if ((timestamp!=lastTimestamp)||(++nanosPart>=10000))
			{
				while (timestamp==lastTimestamp)
				{
					try
					{
						TimeBasedUUIDGenerator.class.wait(1l);
					}
					catch (InterruptedException e)
					{
					}
					timestamp = ((System.currentTimeMillis()+EPOCH_OFFSET_MILLIS)*10000) & 0x0FFFffffFFFFffffl;
				}
				lastTimestamp = timestamp;
				nanosPart = 0;
			}
			timestamp+=nanosPart;
		}
		return timestamp;
	}


	public static String toPrettyString(UUID u) {
		StringBuilder result = new StringBuilder();
		DateFormat sdf = SimpleDateFormat.getDateTimeInstance();
		result.append("UUID [Node:" + Long.toHexString(u.node()) + ",");
		result.append("Variant:" + u.variant() + ",");
		result.append("ClockSeq:" + u.clockSequence() + ",");
		result.append("Version:" + u.version() + ",");
		result.append("Times:" + sdf.format(new Date((u.timestamp()/10000)-EPOCH_OFFSET_MILLIS)) + "]");
		return result.toString();
	}
	

}
