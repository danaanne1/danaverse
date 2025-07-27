package com.ddougher.documentstore;

import java.lang.annotation.*;

@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Getter {

	public String value();
	
}
