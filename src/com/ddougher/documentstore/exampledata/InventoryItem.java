package com.ddougher.documentstore.exampledata;

import com.ddougher.documentstore.DocumentView;
import com.ddougher.documentstore.Getter;
import com.ddougher.documentstore.Setter;

public interface InventoryItem extends DocumentView {
	
	@Getter("Name") public String name();
	@Setter("Name") public void name(String value);
	public InventoryItem withName(String value);

}
