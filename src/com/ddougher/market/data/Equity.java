package com.ddougher.market.data;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Indirect;
import com.theunknowablebits.proxamic.Setter;

public interface Equity extends DocumentView {

	@Getter("symbol") String symbol();

	@Setter("symbol") Equity symbol(String value);
	
	@Getter("technicals") Map<String,Metric> metrics();
	
	default Metric metric(String name) { return metrics().get(name); }

	public interface Metric extends DocumentView {
	
		@Indirect @Getter("years") Map<String,Year> years();
	
		default Year year(int yearNumber) { return years().get(Integer.toString(yearNumber)); }

	}

	public interface Year extends DocumentView {

		/** per day data, keyed by TRADING_DAY_OF_YEAR */
		@Getter("days") Map<String, Day> days();
		
		default Day day(int dayNumber) { return days().get(Integer.toString(dayNumber)); }
		
	}

	public interface Day extends DocumentView {

		@Getter("value") Float [] value();
		
		/** 930am to 4pm (330 minutes) */
		@Getter("minutes") List<Float []> minutes();

		default Float [] minute(int minute) { return minutes().get(minute); }
		
		/** 4am to 930am (390 minutes) */
		@Getter("premarket") List<Float []> premarket();
		
		default Float [] premarket(int minute) { return premarket().get(minute); }

		/** 4pm to 8pm (240 minutes) */
		@Getter("postmarket") List<Float []> postmarket();

		default Float [] postmarket(int minute) { return postmarket().get(minute); }
	}

}
