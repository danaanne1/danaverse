package com.ddougher.documentstore;

import java.io.Serializable;

/** All view classes extend this interface */ 
public interface DocumentView extends Serializable {
	
	Document document();

	
}
