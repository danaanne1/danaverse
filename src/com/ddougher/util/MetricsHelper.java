package com.ddougher.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class MetricsHelper {
	public ConcurrentHashMap<String, AtomicLong> metrics = new ConcurrentHashMap<String, AtomicLong>();

	public <T> T withMetric(String baseName, Supplier<T> action) {
		long timestamp = System.nanoTime();
		T result = action.get();
		getMetric(baseName+".Time").addAndGet(System.nanoTime()-timestamp);
		getMetric(baseName+".Count").addAndGet(1);
		return result;
	};
	
	public void withMetric(String baseName, Runnable action) {
		withMetric(baseName, () -> {
			action.run();
			return null;
		});
	}
	
	public void dumpMetrics() {
		ArrayList<String> al = new ArrayList<String>();
		metrics.keySet().forEach(al::add);
		Collections.sort(al);
		al.forEach(k->{
			if (k.endsWith(".Time")) {
				System.out.println(k + " : " + ((double)metrics.get(k).get())/1000000);
				System.out.println(k.replace(".Time", ".Avg") + " : " + ((double)(metrics.get(k).get()/metrics.get(k.replace(".Time", ".Count")).get()))/1000000);
			} else {
				System.out.println(k + " : " + metrics.get(k).get());
			}
		});
	}
	
	private AtomicLong getMetric(String metricName) {
		AtomicLong l = metrics.get(metricName);
		if (l == null) {
			metrics.putIfAbsent(metricName, new AtomicLong());
			l = metrics.get(metricName);
		}
		return l;
	}

}