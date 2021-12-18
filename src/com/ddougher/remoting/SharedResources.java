package com.ddougher.remoting;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.theunknowablebits.proxamic.TimeBasedUUIDGenerator;

public final class SharedResources {

	public static final ExecutorService cachedThreadPool = Executors.newCachedThreadPool();
	public static final TimeBasedUUIDGenerator UUIDGenerator = TimeBasedUUIDGenerator.instance();
}
