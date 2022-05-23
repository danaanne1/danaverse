package com.ddougher.market.data;

import java.util.Map;

import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Indirect;

public interface Stocks extends DocumentView {

	/** A map from type code to stock family */
	@Indirect @Getter("families") Map<String,Family> families();

	default Family family(String familyName) {
		Map<String,Family> families = families();
		if (!families.containsKey(familyName))
			families.put(familyName, document().newInstance(Family.class));
		return families.get(familyName);
	}
	
	/** Stock Family */
	public interface Family extends DocumentView {
		
		/** All of the stock tickers */
		@Indirect @Getter("tickers") Map<String,Equity> active();

		@Indirect @Getter("tickers") Map<String,Equity> inactive();
	
	}
	
	
}
