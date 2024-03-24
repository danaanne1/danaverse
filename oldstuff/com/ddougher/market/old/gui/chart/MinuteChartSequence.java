package com.ddougher.market.gui.chart;

import java.io.Closeable;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import com.ddougher.market.Utils;
import com.ddougher.market.data.Equity;
import com.ddougher.market.data.Equity.Day;
import com.ddougher.market.data.MetricConstants;
import com.ddougher.proxamic.MemoryMappedDocumentStore;
import com.ddougher.proxamic.ObservableDocumentStore;
import com.theunknowablebits.proxamic.DocumentStore;
import com.theunknowablebits.proxamic.DocumentStoreAware;

// premarket hours are 4am to 930am (330 minutes)
// regularmarket hours are 930am to 4pm (390 minutes)
// afterhours hours are 4pm to 8pm (240 minutes)
// trading day = 330 + 390 + 240 = 960
public class MinuteChartSequence extends AbstractChartSequence<Float[]> implements Closeable {
	private static final int [] year_days = MetricConstants.year_days;
	
	Equity source;
	String metricName;
	int length;
	
	int startYear;
	int startDay;
	int startMinute;
	boolean premarket = true;
	boolean postmarket = true;
	int minutesperDay = ((premarket==true)?330:0 ) + 390 + ((postmarket==true)?240:0);
	
	Map<String, ObservableDocumentStore.Listener> listeners = new HashMap<String, ObservableDocumentStore.Listener>();

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
		
		Utils.withObservableDocStoreFromDocument(equity.document(), ds ->{
			Calendar c = Calendar.getInstance(TimeZone.getTimeZone("America/New_York"));
			ds.addListener(
					ds.getID(
							equity
								.metric(metricName)
								.year(c.get(Calendar.YEAR))
								.day(MetricConstants.tradingDayFromDate(c.getTime()))
								.document()
					),
					new ObservableDocumentStore.Listener() {
						// TODO: REDO
						// the chart is expected to redraw its entirety in response to these events:
						@Override public void documentPut(DocumentEvent event) { 
							Day d = event.document().as(Day.class);
							int diff = 0;
							if (length > 0)
								fireValueChangedEvent(length-1, 1);
							if (diff != 0) {
								MinuteChartSequence.this.length += diff;
								fireSizeChangedEvent(diff);
							}
						}
						@Override public void documentDeleted(DocumentEvent event) { /* ignored*/ }
					}
			);
			
		});
			
		
	}

	@SuppressWarnings("resource")
	public void close() {
		if (!DocumentStoreAware.class.isAssignableFrom(source.document().getClass())) return;
		DocumentStore store = ((DocumentStoreAware)source.document()).getDocumentStore();
		if (!ObservableDocumentStore.class.isAssignableFrom(store.getClass())) return;
		ObservableDocumentStore mds = (MemoryMappedDocumentStore)store;
		synchronized(listeners) {
			listeners.forEach((key,value)-> mds.removeListener(key, value));
			listeners.clear();
		}
	}
	
	@Override
	public Float [] get(int offset) {
		
		int offset_minutes = offset+startMinute;
		int minute = offset_minutes%minutesperDay;
		int offset_day  = startDay + (offset_minutes/minutesperDay);
		int offset_year = startYear;
		while (offset_day > MetricConstants.year_days[offset_year-2000]) {
			offset_day -= MetricConstants.year_days[offset_year-2000];
			offset_year += 1;
		}
		
		if (!premarket) {
			minute += 330;
		}
		return source.metric(metricName).year(offset_year).day(offset_day).minute(minute);
	}
	
	
	@Override
	public int size() {
		return length;
	}
	
	
}