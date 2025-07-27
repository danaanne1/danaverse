package com.ddougher.documentstore.exampledata;

import com.ddougher.documentstore.DocumentView;
import com.ddougher.documentstore.Getter;

import java.util.List;

public interface CharacterHistory extends DocumentView {

	@Getter("Records") public List<CharacterHistoryRecord> records();
	
}
