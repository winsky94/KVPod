package com;

import com.bigData.KeyGenerator;
import com.bigData.MapUtil;
import com.bigData.winsky.MyProcessor;

import java.util.*;

/**
 * author: winsky
 * date: 2017/10/31
 * description:
 */
public class Tester {
    private static final String[] HOSTS = {"http://172.17.195.115:8501", "http://172.17.195.115:8500", "http://172.17.195.115:8502"};
    private static List<String> keys;
    private static ArrayList<Map<String, String>> list;
    private static Random random = new Random();
    private static int maxRetry = 5;
    private static int size = 10000;
    private static int perSize = 1000;
    // private static int size = 1000000;
    // private static int perSize = 5000;
    private static int batchSize = 500;

    public static void main(String[] args) throws Exception {
        init();

        // testPut();
        testGet();

        // testBatchPut();
        // testRandomGet();

        // System.out.println(keys.get(9999));
        // doGet("1ddtco6vcp7kqa9xlfkdw5s6eoq0i3so");

        // myTest();
    }

    private static void myTest() {
        MyProcessor processor = new MyProcessor();
        for (int i = 0; i < 100; i++) {
            // int idx = i;
            // new Thread() {
            //     @Override
            //     public void run() {
            //         int start = idx * 100;
            //         for (int j = idx * 100; j < start + 100; j++) {
            //             processor.put(keys.get(j), list.get(j));
            //             System.out.println(System.currentTimeMillis());
            //         }
            //     }
            // }.start();

            Map<String, String> map = processor.get(keys.get(i));
            if (map==null){
                System.out.println("null");
            }else {
                System.out.println(map.get("idx"));
            }
        }
    }

    private static void init() {
        keys = KeyGenerator.getKeyFromFile(size);

        list = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            Map<String, String> value = new HashMap<>();
            value.put("idx", i + "");
            value.put("name", "lucy");
            value.put("age", "12");
            value.put("sex", "female");
            value.put("luckyNumber", "25");
            value.put("havingCat", "yes");
            value.put("catAge", "1");
            value.put("profile", "iloveCatsundayday.Heiscute.Hehasnoeggs.fsdfsd");
            value.put("city", "suzhou");
            value.put("attr", "no");
            list.add(value);
        }
    }

    private static void testBatchPut() throws Exception {
        System.out.println("**********************开始测试批量写数据**********************");

        for (int i = 0; i < size / perSize; i++) {
            int start = i * perSize;
            new Thread(() -> {
                for (int j = 0; j < perSize / batchSize; j++) {
                    try {
                        int retry = 0;
                        int s = start + j * batchSize;
                        Map<String, Map<String, String>> data = new HashMap<>();
                        for (int k = s; k < s + batchSize; k++) {
                            data.put(keys.get(k), list.get(k));
                        }
                        String content = null;
                        String param = MapUtil.map2String(data);
                        while (!"success!".equals(content) && retry < maxRetry) {
                            retry++;
                            String url = getHost() + "/batchProcess";
                            try {
                                content = HttpUtil.post(url, param);
                            } catch (Exception e) {
                                System.out.println(url + " " + e.getMessage());
                            }
                        }
                        if (retry == maxRetry) {
                            System.out.println("到达最大重试次数，param=" + param);
                        } else if (retry != 1) {
                            System.out.println("retry " + retry + "次数");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }
            }).start();
        }
    }

    private static void testPut() throws Exception {
        System.out.println("**********************开始测试多线程写数据**********************");

        for (int i = 0; i < size / perSize; i++) {
            int start = i * perSize;
            // new MyThread(start, perSize).start();

            new Thread(() -> {
                for (int j = start; j < start + perSize; j++) {
                    try {
                        int retry = 0;
                        String content = null;
                        Map<String, Object> paramMap = new HashMap<>();
                        paramMap.put("key", keys.get(j));
                        paramMap.put("value", list.get(j));
                        String param = MapUtil.map2String(paramMap);
                        while (!"success!".equals(content) && retry < maxRetry) {
                            retry++;
                            String url = getHost() + "/process";
                            try {
                                content = HttpUtil.post(url, param);
                            } catch (Exception e) {
                                System.out.println("url=" + url + " " + e.getMessage());
                            }
                        }
                        if (retry == maxRetry) {
                            System.out.println("到达最大重试次数，param=" + param);
                        } else if (retry != 1) {
                            System.out.println("retry " + retry + "次数");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    private static void testGet() throws Exception {
        System.out.println("**********************开始测试读取数据**********************");

        for (int i = 0; i < size / perSize; i++) {
            int idx = i;
            new Thread(() -> {
                int start = idx * perSize;
                for (int j = start; j < start + perSize; j++) {
                    String key = keys.get(j);
                    doGet(key);
                }

            }).start();
        }
    }

    private static void testRandomGet() throws Exception {
        int num = 10000;
        List<String> targetKeys = randomKeys(num);
        System.out.println("**********************开始测试随机读取数据**********************");

        for (int i = 0; i < 100; i++) {
            int start = i * 100;
            new Thread(() -> {
                // 100个线程，每个线程随机读100个
                for (int j = start; j < start + 100; j++) {
                    String key = targetKeys.get(j);
                    doGet(key);
                }
            }).start();

        }
    }

    private static List<String> randomKeys(int num) {
        List<String> result = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            int index = random.nextInt(size);
            String key = keys.get(index);
            result.add(key);
        }
        return result;
    }

    private static void doGet(String key) {
        try {
            String url = getHost() + "/process";
            // String url = HOSTS[0] + "/process";
            Map<String, String> headers = new HashMap<>();
            headers.put("key", key);
            String content = HttpUtil.get(url, headers);
            if ("find nothing".equals(content)) {
                System.out.println("key " + key + " find nothing");
            } else {
                // System.out.println(content);
            }
        } catch (Exception e) {
            System.out.println("exception key:" + key + " , " + e.getMessage());
        }
    }

    private static String getHost() {
        int idx = random.nextInt(HOSTS.length);
        // idx = 2;
        return HOSTS[idx];
    }
}
