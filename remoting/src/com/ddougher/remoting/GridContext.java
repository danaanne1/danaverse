package com.ddougher.remoting;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class GridContext {

    public static final Map<String, Object> context = new ConcurrentHashMap<String, Object>();
    
    static {
        // Register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("GridContext: Shutdown detected, performing cleanup...");
            performShutdownActions();
        }));
    }
    
    private static void performShutdownActions() {
        // Add your shutdown logic here
        System.out.println("GridContext: Clearing context...");

        context.values().parallelStream().forEach(v -> {
            if (v instanceof Closeable) {
                try {
                    ((Closeable) v).close();
                } catch (IOException e) {
                    System.err.println("GridContext: Error closing context value: " + v);
                }
            }
        });

        System.out.println("GridContext: Shutdown cleanup completed.");
    }
}