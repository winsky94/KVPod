package com.bigData.winsky;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * author: winsky
 * date: 2017/11/9
 * description:
 */
public class LogUtil {
    private static final String LOG_DIR = "/opt/localdisk/";

    public static void log(String key, String val) {
        try {
            FileWriter fileWriter = new FileWriter(new File(LOG_DIR + key));
            fileWriter.write(val);
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteLog(String key) {
        File file = new File(LOG_DIR + key);
        file.delete();
    }

    public static Map<String, String> readLog() {
        File dir = new File(LOG_DIR);
        String[] files = dir.list();
        Map<String, String> map = new HashMap<>();
        for (String key : files) {
            try {
                String val = read(buildPath(key));
                map.put(key, val);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return map;
    }

    public static String read(String path) throws IOException {
        File file = new File(path);
        BufferedReader reader = new BufferedReader(new FileReader(file));
        return reader.readLine();
    }

    public static Map<String, String> getLocalFiles() {
        File dir = new File(LOG_DIR);
        String[] files = dir.list();
        Map<String, String> map = new HashMap<>();
        for (String key : files) {
            map.put(key, buildPath(key));
        }
        return map;
    }

    private static String buildPath(String key) {
        return LOG_DIR + key;
    }
}
