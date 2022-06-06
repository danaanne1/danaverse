package com.ddougher.market.data;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

public class MetricConstants {

	/** number of weekdays per year, starting with 2000 */ 
	public static final int [] year_days = new int [] { 
			260, 261, 261, 261, 262, 
			260, 260, 261, 262, 262, 
			261, 260, 261, 261, 261, 
			261, 261, 260, 261, 261, 
			262, 261, 260, 260, 262,
			261, 261, 261, 260, 261
	};

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


	public static int tradingDayFromDate(Date startTime) {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		c.setTime(startTime);
		int week_of_year = c.get(Calendar.WEEK_OF_YEAR);
		int day_of_week = c.get(Calendar.DAY_OF_WEEK);
		if (day_of_week == Calendar.SUNDAY)
			day_of_week = 0;
		else if (day_of_week == Calendar.SATURDAY)
			day_of_week = 5;
		else 
			day_of_week -=2;
		int year = c.get(Calendar.YEAR);
		c.clear();
		c.set(year, 0, 1, 4, 0, 0);
		int offset = c.get(Calendar.DAY_OF_WEEK);
		if (offset == Calendar.SUNDAY) 
			offset = 0;
		else if (offset == Calendar.SATURDAY) 
			offset = 5;
		else
			offset = offset - 2;
		return (week_of_year*5) + day_of_week - offset;
	}


	public static int tradingMinuteFromDate(Date startTime) {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		c.setTime(startTime);
		int minutes = (c.get(Calendar.HOUR_OF_DAY) * 60) + c.get(Calendar.MINUTE);
		return Math.min(960, Math.max(0, minutes-240));
	}


	public static int tradingYearFromDate(Date startTime) {
		Calendar c = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
		c.setTime(startTime);
		return c.get(Calendar.YEAR);
	}

	public static Date previousTradingDate() {
		Date d = new Date();
		int year = tradingYearFromDate(d);
		int day = tradingDayFromDate(d);
		int minute = tradingMinuteFromDate(d);
		
		day -= 1;
		if (day < 0 ) {
			year -= 1;
			day = year_days[year-2000]-1;
		}
		
		return calendarDateFromTradingOffset(year, day, minute);
	}

	
}
