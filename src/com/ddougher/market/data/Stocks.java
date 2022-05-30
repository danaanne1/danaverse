package com.ddougher.market.data;

import java.util.Map;

import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Indirect;

public interface Stocks extends DocumentView {

	/** All of the stock tickers */
	@Indirect @Getter("tickers") Map<String,Equity> tickers();
	
	
}
