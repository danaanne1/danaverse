package com.ddougher.market.data;

import java.util.Map;

import com.theunknowablebits.proxamic.DocumentView;
import com.theunknowablebits.proxamic.Getter;
import com.theunknowablebits.proxamic.Setter;

public interface Aggregate extends DocumentView {
	
	@Getter("ohlc") Float [] ohlcva();
	@Setter("ohlc") Float [] ohlcva(Float [] val);
	@Setter("ohlc") Aggregate withOhlcva(Float [] val);


	@Getter("computed") Map<String,Float> technicals();

}
