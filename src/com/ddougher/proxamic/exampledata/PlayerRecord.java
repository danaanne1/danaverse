package com.ddougher.proxamic.exampledata;

import com.ddougher.proxamic.*;

import java.util.List;

public interface PlayerRecord extends DocumentView, DocumentStoreAware {
	
	
	@Getter("PlayerName") String name();
	@Setter("PlayerName") PlayerRecord name(String value);
	
	@Indirect
    @Getter("characters") List<CharacterRecord> characters();

}
