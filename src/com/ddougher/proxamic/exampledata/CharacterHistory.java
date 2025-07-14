package com.ddougher.proxamic.exampledata;

import com.ddougher.proxamic.DocumentView;
import com.ddougher.proxamic.Getter;

import java.util.List;

public interface CharacterHistory extends DocumentView {

	@Getter("Records") public List<CharacterHistoryRecord> records();
	
}
