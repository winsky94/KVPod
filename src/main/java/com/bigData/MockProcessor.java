package com.bigData;

import   cn.helium.kvstore.processor.Processor; 
import   java.util.HashMap;
import java.util.List;
import   java.util.Map;

public class MockProcessor implements Processor {

	Map<String, Map<String, String>> store = new HashMap<String, Map<String, String>>();

	public Map<String, String> get(String key) {
		Map<String, String> record = store.get(key);
		return record;
	}

	public synchronized boolean put(String key, Map<String, String> value) {

		store.put(key, value);
		return true;
	}

	public synchronized boolean batchPut(Map<String, Map<String, String>> records) {
		store.putAll(records);
		return true;
	}

	public byte[] process(byte[] inupt) {
		System.out.println("receive   info:   " + new String(inupt));
		return "received!".getBytes();
	}

	@Override
	public int count(Map<String, String> arg0) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Map<Map<String, String>, Integer> groupBy(List<String> arg0) {
		// TODO Auto-generated method stub
		return null;
	}
}
