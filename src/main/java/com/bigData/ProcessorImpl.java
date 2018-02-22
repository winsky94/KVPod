package com.bigData;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.processor.Processor;
import com.bigData.winsky.MyProcessor;

import java.util.List;
import java.util.Map;

public class ProcessorImpl implements Processor {
    //    private MapList mapList;
    //    private AccessMap accessMap;
    // private DoubleQueue doubleQueue;
    private MyProcessor myProcessor = new MyProcessor();

    public static String hdfsAddr = KvStoreConfig.getHdfsUrl();

    public ProcessorImpl() {
        // mapList = new MapList(10000, 100);
        //        mapList = new MapList(100000, 1000);
        // mapList = new MapList(100, 10);
        //        accessMap = new AccessMap(100000, 20000);
        // doubleQueue = new DoubleQueue(100000);
    }

    @Override
    public Map<String, String> get(String s) {
        // try {
        //     String val = doubleQueue.get(s);
        //     // if (val==null) {
        //     //     System.out.println("读出的是null" );
        //     // }
        //     Map<String, String> result = MapUtil.string2Map(val);
        //     return result != null ? result : new HashMap<>();
        // } catch (Exception e) {
        //     e.printStackTrace();
        //     return new HashMap<>();
        // }
        return myProcessor.get(s);
    }

    @Override
    public boolean put(String s, Map<String, String> map) {
        // try {
        //     String val = MapUtil.map2String(map);
        //     if (val != null) {
        //         return doubleQueue.add(s, val);
        //     }
        //     return false;
        // } catch (Exception e) {
        //     e.printStackTrace();
        //     return false;
        // }
        return myProcessor.put(s, map);
    }

    @Override
    public boolean batchPut(Map<String, Map<String, String>> map) {
        // boolean result;
        // Iterator<Map.Entry<String, Map<String, String>>> iterator = map.entrySet().iterator();
        // while (iterator.hasNext()) {
        //     Map.Entry<String, Map<String, String>> entry = iterator.next();
        //     try {
        //         String val = MapUtil.map2String(entry.readValue());
        //         if (val != null) {
        //             result = doubleQueue.add(entry.getKey(), val);
        //             if (!result) {
        //                 return false;
        //             }
        //         }
        //     } catch (Exception ignored) {
        //     }
        // }
        // return true;
        return myProcessor.batchPut(map);
    }

    @Override
    public byte[] process(byte[] bytes) {
        // String[] objects = MsgUtil.parseMessage(bytes);
        // String type = objects[0];
        // if (type.equals("01")) {
        //     String key = objects[1];
        //     String value = null;
        //     try {
        //         // value = doubleQueue.getFromMap(key);
        //         value = doubleQueue.getFromLocal(key);
        //     } catch (Exception e) {
        //         e.printStackTrace();
        //     }
        //     String result = "null";
        //     if (value != null) {
        //         result = value;
        //     }
        //     return MsgUtil.generateMessage("11", result);
        // }
        // return null;
        return myProcessor.process(bytes);
    }

    @Override
    public int count(Map<String, String> arg0) {
        return 0;
    }

    @Override
    public Map<Map<String, String>, Integer> groupBy(List<String> arg0) {
        return null;
    }
}
