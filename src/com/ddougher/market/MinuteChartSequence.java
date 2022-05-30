package com.ddougher.market;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.ddougher.market.data.Equity;
import com.ddougher.market.data.MetricConstants;
import com.ddougher.proxamic.DocumentListener;
import com.ddougher.proxamic.MemoryMappedDocumentStore;
import com.theunknowablebits.proxamic.DocumentStore;
import com.theunknowablebits.proxamic.DocumentStoreAware;

// premarket hours are 4am to 930am (330 minutes)
// regularmarket hours are 930am to 4pm (390 minutes)
// afterhours hours are 4pm to 8pm (240 minutes)
// trading day = 330 + 390 + 240 = 960
public class MinuteChartSequence extends AbstractChartSequence<Float[]> implements Closeable {
	private static final int [] year_days = new int [] { 
			260, 261, 261, 261, 262, 
			260, 260, 261, 262, 262, 
			261, 260, 261, 261, 261, 
			261, 261, 260, 261, 261, 
			262, 261, 260, 260, 262,
			261, 261, 261, 260, 261
	};
	
	Equity source;
	String metricName;
	int length;
	
	int startYear;
	int startDay;
	int startMinute;
	boolean premarket = true;
	boolean postmarket = true;
	int minutesperDay = ((premarket==true)?330:0 ) + 390 + ((postmarket==true)?240:0);
	
	Map<String, DocumentListener> listeners = new HashMap<String, DocumentListener>();

	/* TODO: The current day always gets a listener */
	public MinuteChartSequence(Equity equity, String metricName, Date startTime, int length, boolean preMarket, boolean postMarket) {
		this.startDay = MetricConstants.tradingDayFromDate(startTime);
		this.startMinute = MetricConstants.tradingMinuteFromDate(startTime);
		if (preMarket)
			this.startMinute -= 330;
		this.startYear = MetricConstants.tradingYearFromDate(startTime);
		this.premarket = preMarket;
		this.postmarket = postMarket;
		this.metricName = metricName;
		this.source = equity;
	}

	public void close() {
		if (!DocumentStoreAware.class.isAssignableFrom(source.document().getClass())) return;
		DocumentStore store = ((DocumentStoreAware)source.document()).getDocumentStore();
		if (!MemoryMappedDocumentStore.class.isAssignableFrom(store.getClass())) return;
		MemoryMappedDocumentStore mds = (MemoryMappedDocumentStore)store;
		synchronized(listeners) {
			listeners.forEach((key,value)-> mds.removeDocumentListener(key, value));
			listeners.clear();
		}
	}
	
	@Override
	public Float [] get(int offset) {
		
		int offset_minutes = offset+startMinute;
		int minute = offset_minutes%minutesperDay;
		int offset_day  = startDay + (offset_minutes/minutesperDay);
		int offset_year = startYear;
		while (offset_day > year_days[offset_year-2000]) {
			offset_day -= year_days[offset_year-2000];
			offset_year += 1;
		}
		
		if (premarket) {
			if (minute < 330) {
				return source.metric(metricName).year(offset_year).day(offset_day).premarket(minute);
			} 
			if (minute < 720) {
				return source.metric(metricName).year(offset_year).day(offset_day).minute(minute-330);
			}
			return source.metric(metricName).year(offset_year).day(offset_day).postmarket(minute-720);
		}
		if (minute < 390) {
			return source.metric(metricName).year(offset_year).day(offset_day).minute(minute);
		}
		return source.metric(metricName).year(offset_year).day(offset_day).postmarket(minute-390);
	}
	
	
	@Override
	public int size() {
		return length;
	}
	
	
}
