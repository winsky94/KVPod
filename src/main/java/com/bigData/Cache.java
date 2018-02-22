package com.bigData;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Cache {
	private static Queue<Map<String, String>> cache = new ConcurrentLinkedQueue<Map<String, String>>();
    private static final int MAX_SIZE = 10;

    public static synchronized void add(Map<String, String> map) {
        while (cache.size() >= MAX_SIZE) {
            cache.poll();
        }
        cache.add(map);
    }

    public static String get(String key) {
        Iterator<Map<String, String>> iterator = cache.iterator();
        while (iterator.hasNext()) {
            Map<String, String> map = iterator.next();
            String val = map.get(key);
            if (val != null) {
                return val;
            }
        }
        return null;
    }
}
