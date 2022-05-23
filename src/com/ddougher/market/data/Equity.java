package com.ddougher.market.data;

import java.util.List;
import java.util.Map;

import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Indirect;
import com.theunknowablebits.proxamic.Setter;

public interface Equity extends DocumentView {

	@Getter("symbol") String symbol();
	@Getter("name") String name();
	@Getter("type") String type();
	@Getter("exchange") String exchange();
	@Getter("locale") String locale();
	@Getter("active") Boolean active();
	
	@Setter("symbol") Equity withSymbol(String value);
	@Setter("name") Equity withName(String value);
	@Setter("type") Equity withType(String type);
	@Setter("exchange") Equity withExchange(String value);
	@Setter("locale") Equity withLocale(String value);
	@Setter("active") Equity withActive(Boolean value);
	
	@Indirect @Getter("technicals") Map<String,Metric> metrics();
	
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

		@Getter("minutes") List<Float []> minutes();
		@Getter("premarket") List<Float []> premarket();
		@Getter("postmarket") List<Float []> postmarket();

		default Float [] minute(int minute) { return minutes().get(minute); }
		default Float [] premarket(int minute) { return premarket().get(minute); }
		default Float [] postmarket(int minute) { return postmarket().get(minute); }

	}

}
