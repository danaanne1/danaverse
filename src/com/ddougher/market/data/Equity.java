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

		/** per day data, keyed by DAY_OF_YEAR (0 to 365) */
		@Indirect @Getter("days") Map<String, Day> days();

	}

	public interface Day extends Metric {

		/** 930am to 4pm (330 minutes) */
		@Getter("minutes") List<Metric> minutes();

		/** 4am to 930am (330 minutes) */
		@Getter("premarket") List<Metric> premarket();
		
		/** 4pm tp 8pm (240 minutes) */
		@Getter("postmarket") List<Metric> postmarket();

	}

	public interface Metric extends DocumentView {

		@Getter("v") Float [] values();
		@Setter("v") Float [] values(Float [] values);
		
		@Getter("t") Map<String,Float []> technicals();

	}

	public interface MetricConstants {
		public static final int OPEN = 0;
		public static final int HIGH = 1;
		public static final int LOW = 2;
		public static final int CLOSE = 3;
		public static final int VOLUME = 4;
		public static final int AVERAGE = 5;

		public static final int _COUNT = 6;
	}

}
