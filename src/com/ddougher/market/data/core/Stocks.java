package com.ddougher.market.data.core;

import java.util.Map;

import com.theunknowablebits.proxamic.DocumentStoreAware;
import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Indirect;

public interface Stocks extends DocumentView, DocumentStoreAware {

	/** All of the stock tickers */
	@Indirect @Getter("tickers") Map<String,Equity> tickers();
	
	
}
