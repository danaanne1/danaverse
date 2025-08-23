package com.ddougher.market.data;

import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class MetricConstants {


	/** Definitions for the built in candle value array */
	public enum Candle {
		TIME(0, "Window start time"),
		OPEN(1, "The starting price"),
		HIGH(2, "The higest price reached"),
		LOW(3, "The lowest price reached"),
		CLOSE(4, "The ending price"),
		VOLUME(5, "Number of shares traded"),
		VWAP(6, "The volume weighted average price"),
		TRADESIZE(7, "The number of trades"),
		VWMA(8, "Volume weighted moving average");

		
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
		technicalFieldList.put("ohlc_min", new String []  { "time", "open", "high", "low", "close", "volume", "vwap", "tradesize", "vwma" } ); // OHLC
	}

	
	public static final TimeZone NEW_YORK = TimeZone.getTimeZone("America/New_York");
	

	
}
