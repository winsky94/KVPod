package com.bigData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * author: winsky
 * date: 2017/11/7
 * description:
 */
public class JobCenter {
    private static final int MAX_FILE_SIZE = 3;
    private static final String LOCAL_DIR = "/opt/localdisk/sstable";
    private static Queue<String> jobs = new ConcurrentLinkedQueue<>();

    public static synchronized boolean isAvailable(String file) {
        boolean available = jobs.size() < MAX_FILE_SIZE;
        if (available) {
            return true;
        } else {
            Iterator<String> it = jobs.iterator();
            while (it.hasNext()) {
                if (it.next().equals(file)) {
                    return true;
                }
            }
            return false;
        }
    }

    public static synchronized void add(String file) {
        boolean exist = false;
        try {
            Iterator<String> it = jobs.iterator();
            while (it.hasNext()) {
                if (it.next().equals(file)) {
                    exist = true;
                }
            }
            if (!exist) {
                jobs.offer(file);
            }
        } catch (Exception ignored) {
        }
    }

    public static void start() {
        new Thread(() -> {
            while (true) {
                try {
                    String file = jobs.peek();
                    if (file != null) {
                        doJob(file);
                    }
                    Thread.sleep(10);
                } catch (Exception ignored) {
                }
            }
        }).start();
    }

    private static void doJob(String fileName) throws Exception {
        Map<String, String> newMapList = new HashMap<>();

        File newFile = new File(fileName);
        FileReader fd = new FileReader(newFile);
        BufferedReader bf = new BufferedReader(fd);
        String tempKey;
        String tempValue;

        while ((tempKey = bf.readLine()) != null) {
            tempValue = bf.readLine();
            newMapList.put(tempKey, tempValue);
        }
        Cache.add(newMapList);

        // 加到内存中后删除本地文件和任务队列
        deleteFile(fileName);
        jobs.poll();
    }

    private static void deleteFile(String fileName) {
        synchronized (MapList.readLock) {
            File file = new File(LOCAL_DIR + "/" + fileName);
            if (file.exists() && file.isFile()) {
                file.delete();
            }

            File tempFile = new File(LOCAL_DIR + "/." + fileName + ".crc");
            if (tempFile.exists() && tempFile.isFile()) {
                tempFile.delete();
            }
        }
    }
}
