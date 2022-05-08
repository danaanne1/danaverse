package com.ddougher.market.data;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MetricConstants {

	/** Definitions for the built in candle value array */
	public enum Candle {
		OPEN(0, "The starting price"),
		HIGH(1, "The higest price reached"),
		LOW(2, "The lowest price reached"),
		CLOSE(3, "The ending price"),
		VOLUME(4, "Number of shares traded"),
		VWAP(5, "The volume weighted average price");
		
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
		technicalFieldList.put("ohlc", new String []  { "open", "high", "low", "close", "volume", "vwap" } ); // OHLC
	}

	
	/**
	 * 
	 * @param year
	 * @param day trading day (not counting holidays)
	 * @param minute (count of minutes from 4am)
	 * @return
	 */
	public static final Date calendarDateFromTradingOffset(int year, int day, int minute) {
		Calendar c = Calendar.getInstance();
		c.clear();
		c.set(year, 0, 1, 4, 0, 0);
		int offset = c.get(Calendar.DAY_OF_WEEK);
		if (offset == Calendar.SUNDAY) {
			offset = 0;
		} else if (offset == Calendar.SATURDAY) {
			offset = 5;
		} else {
			offset = offset - 2;
		}
		c.add(Calendar.DAY_OF_WEEK,Calendar.MONDAY-c.get(Calendar.DAY_OF_WEEK)); // zero to monday
		c.add(Calendar.MINUTE, minute);
		c.add(Calendar.WEEK_OF_YEAR, (day+offset)/5);
		c.add(Calendar.DAY_OF_WEEK, ((day+offset)%5));
		return c.getTime();
	}

}
