package com.ddougher.market.data;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class MetricConstants {


	/** Definitions for the built in candle value array */
	public enum Candle {
		OPEN(0, "The starting price"),
		HIGH(1, "The higest price reached"),
		LOW(2, "The lowest price reached"),
		CLOSE(3, "The ending price"),
		VOLUME(4, "Number of shares traded"),
		VWAP(5, "The volume weighted average price"),
		TRADES(6, "The number of trades");
		
		public final int value;
		public final String description;

		Candle(int value , String description) {
			this.value = value;
			this.description = description;
		}
	}

	/** 
	 *  Definitions for the names of the values in technicals.
	 *  For each technical, will contain a list of field names corresponding to the fields in the metric array for that technical
	 */
	public static final Map<String, String []> technicalFieldList = new ConcurrentHashMap<String, String[]>();
	
	static {
		technicalFieldList.put("ohlc", new String []  { "open", "high", "low", "close", "volume", "vwap", "trades" } ); // OHLC
	}

	
	public static final TimeZone NEW_YORK = TimeZone.getTimeZone("America/New_York");
	

	
}
