package com.bigData.winsky;

import java.util.HashMap;
import java.util.Map;

/**
 * author: winsky
 * date: 2017/11/9
 * description:
 */
public class MyProcessor {
    public final static Map<String, Map<String, String>> mem = new HashMap<>();

    public MyProcessor() {
        ReadThread.begin();
        WriteThread.begin();
    }

    public boolean put(String key, Map<String, String> value) {
        synchronized (mem) {
            mem.put(key, value);
        }
        return true;
    }

    public Map<String, String> get(String key) {
        Map<String, String> val = ReadThread.getValue(key);
        if (val != null) {
            return val;
        } else {
            return ReadThread.readValue(key);
        }
    }

    public boolean batchPut(Map<String, Map<String, String>> map) {
        synchronized (mem) {
            mem.putAll(map);
        }
        return true;
    }

    public byte[] process(byte[] msg) {
        return null;
    }
}
