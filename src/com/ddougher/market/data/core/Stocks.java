package com.ddougher.market.data.core;

import java.util.Map;

import com.ddougher.proxamic.DocumentStoreAware;
import com.ddougher.proxamic.DocumentView;
import com.ddougher.proxamic.Getter;
import com.ddougher.proxamic.Indirect;

public interface Stocks extends DocumentView, DocumentStoreAware {

	/** All of the stock tickers */
	@Indirect @Getter("tickers") Map<String,Equity> tickers();
	
	
}
