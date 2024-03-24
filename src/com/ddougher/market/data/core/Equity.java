package com.ddougher.market.data.core;

import java.util.List;
import java.util.Map;

import com.theunknowablebits.proxamic.*;

public interface Equity extends DocumentView, DocumentStoreAware {

	@Getter("symbol") String getSymbol();
	@Getter("name") String getName();
	@Getter("type") String getType();
	@Getter("exchange") String getExchange();
	@Getter("locale") String getLocale();
	@Getter("active") Boolean getActive();
	
	@Setter("symbol") Equity setSymbol(String value);
	@Setter("name") Equity setName(String value);
	@Setter("type") Equity setType(String type);
	@Setter("exchange") Equity setExchange(String value);
	@Setter("locale") Equity setLocale(String value);
	@Setter("active") Equity setActive(Boolean value);
	
	@Indirect @Getter("technicals") Map<String,Metric> getMetrics();

	public interface Metric extends DocumentView {
	
		@Indirect @Getter("years") Map<String,Year> getYears();

	}

	public interface Year extends DocumentView {

		@Getter("days") Map<String, Day> getDays();
		
	}

	public interface Day extends DocumentView {

		@Getter("values") List<Number []> getValues(); // test proves this works

	}

}
