package com.ddougher.remoting;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class SharedResources {

	public static final ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	
}
