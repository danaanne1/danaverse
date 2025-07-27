package com.ddougher.documentstore.exampledata;

import com.ddougher.documentstore.*;

import java.util.List;

public interface PlayerRecord extends DocumentView, DocumentStoreAware {
	
	
	@Getter("PlayerName") String name();
	@Setter("PlayerName") PlayerRecord name(String value);
	
	@Indirect
    @Getter("characters") List<CharacterRecord> characters();

}
