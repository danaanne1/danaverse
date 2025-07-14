package com.ddougher.proxamic.exampledata;

import com.ddougher.proxamic.DocumentView;
import com.ddougher.proxamic.Getter;
import com.ddougher.proxamic.Setter;

public interface InventoryItem extends DocumentView {
	
	@Getter("Name") public String name();
	@Setter("Name") public void name(String value);
	public InventoryItem withName(String value);

}
