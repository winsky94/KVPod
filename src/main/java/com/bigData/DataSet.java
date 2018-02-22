package com.bigData;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * author: winsky
 * date: 2017/11/2
 * description:
 */
@Deprecated
public class DataSet {
    // private static Lock lock = new ReentrantLock();
    private static ConcurrentSkipListMap<String, String> mapList = create();

    private static ConcurrentSkipListMap<String, String> create() {
        return new ConcurrentSkipListMap<>(new Comparator<String>() {
            public int compare(String s1, String s2) {
                return s1.compareTo(s2);
            }
        });
    }

    public static boolean isEmpty() {
        return mapList.isEmpty();
    }

    public static String get(String key) {
        // lock.lock();
        String val = mapList.get(key);
        // lock.unlock();
        return val;
    }

    public static void put(String key, String val) {
        // lock.lock();
        mapList.put(key, val);
        // lock.unlock();
    }

    public static ConcurrentSkipListMap<String, String> cloneMap() {
        // lock.lock();
        ConcurrentSkipListMap<String, String> result = mapList.clone();
        mapList = create();
        // lock.unlock();
        return result;
    }

    public static int size() {
        return mapList.size();
    }
}
