package com.ddougher.market.data.core;

import java.util.Map;

import com.ddougher.documentstore.DocumentStoreAware;
import com.ddougher.documentstore.DocumentView;
import com.ddougher.documentstore.Getter;
import com.ddougher.documentstore.Indirect;

public interface Stocks extends DocumentView, DocumentStoreAware {

	/** All of the stock tickers */
	@Indirect @Getter("tickers") Map<String,Equity> tickers();
	
	
}
