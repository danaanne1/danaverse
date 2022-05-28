package com.ddougher.market;

import java.util.Date;

import com.ddougher.market.data.Equity;

// premarket hours are 4am to 930am (330 minutes)
// regularmarket hours are 930am to 4pm (390 minutes)
// afterhours hours are 4pm to 8pm (240 minutes)
public class MinuteChartSequence extends AbstractChartSequence<Float[]> {
	private static final int [] year_days = new int [] { 
			262, 262, 262, 262, 262, 
			262, 262, 262, 262, 262, 
			262, 262, 262, 262, 262, 
			262, 262, 262, 262, 262, 
			262, 262, 262, 262, 262
	};
	
	Equity source;
	String metric;
	int length;
	
	int startYear;
	int startDay;
	int startMinute;
	boolean premarket = true;
	boolean postmarket = true;
	int minutesperDay = ((premarket==true)?330:0 ) + 390 + ((postmarket==true)?240:0);
	
	@Override
	public Float [] get(int offset) {
		
		int offset_minutes = offset+startMinute;
		int minute = offset_minutes%minutesperDay;
		int offset_day  = startDay + (offset_minutes/minutesperDay);
		int offset_year = startYear;
		while (offset_day > year_days[offset_year-2000]) {
			offset_day -= year_days[offset_year];
			offset_year += 1;
		}
		
		if (premarket) {
			if (minute < 330) {
				return source.metric(metric).year(offset_year).day(offset_day).premarket(minute);
			} 
			if (minute < 720) {
				return source.metric(metric).year(offset_year).day(offset_day).minute(minute-330);
			}
			return source.metric(metric).year(offset_year).day(offset_day).postmarket(minute-720);
		}
		if (minute < 390) {
			return source.metric(metric).year(offset_year).day(offset_day).minute(minute);
		}
		return source.metric(metric).year(offset_year).day(offset_day).postmarket(minute-390);
	}
	
	
	@Override
	public int size() {
		return length;
	}
	
	
}
