package com.ddougher.market.data;

import java.util.List;
import java.util.Map;

import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Indirect;
import com.theunknowablebits.proxamic.Setter;

public interface Equity extends DocumentView {

	@Getter("symbol") String symbol();
	@Setter("symbol") Equity symbol(String value);
	
	@Indirect @Getter("years") Map<String,Year> years();

	public interface Year extends DocumentView {

		/** per day data, keyed by MONTH_OF_YEAR_2+DAY_OF_MONTH_2 , ie 0101 */
		@Indirect @Getter("days") Map<String, Day> days();

	}

	public interface Day extends Candle {

		/** the list of "market open" 1 minute candles, minute 0 is the candle that goes from 930am to 931am */
		@Getter("candles") List<Candle> candles();

		/** the list of premarket candles */
		@Getter("premarket") List<Candle> premarket();
		
		/** the list of postmarket candles */
		@Getter("postmarket") List<Candle> postmarket();

	}

	public interface Candle extends DocumentView {
		
		@Getter("values") Float [] value();
		@Setter("values") Float [] value(Float [] values);

	}

	public interface Constants {
		public static final int OPEN = 0;
		public static final int HIGH = 1;
		public static final int LOW = 2;
		public static final int CLOSE = 3;
		public static final int VOLUME = 4;
		public static final int AVERAGE = 5;
	}

}
