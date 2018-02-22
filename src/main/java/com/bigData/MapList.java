package com.bigData;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import org.apache.hadoop.fs.FileStatus;

import java.io.*;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

public class MapList {
    private static ConcurrentSkipListMap<String, String> mapList;
    private int maxSize = 10000;
    private int indexInterval = 100;
    private BufferedWriter logWriter;
    private int totalKVPodNums;
    private int currentID;

    private String localDir = "/opt/localdisk/sstable";
    private String log = "/opt/localdisk/log.txt";
    private boolean isFlush = false;
    public static Object readLock = new Object();

    public MapList(int maxSize, int indexInterval) {
        totalKVPodNums = KvStoreConfig.getServersNum();
        currentID = RpcServer.getRpcServerId();

        this.maxSize = maxSize;
        this.indexInterval = indexInterval;

        mapList = new ConcurrentSkipListMap<String, String>(new Comparator<String>() {
            public int compare(String s1, String s2) {
                return s1.compareTo(s2);
            }
        });

        File file = new File(log);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        File sf = new File(localDir);
        if (!sf.exists() || !sf.isDirectory()) {
            sf.mkdir();
        }

        try {
            initLogWriter();
            checkLog();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void checkLog() throws Exception {
        BufferedReader logReader = new BufferedReader(new FileReader(log));
        String tempKey = "";
        String tempValue = "";
        while ((tempKey = logReader.readLine()) != null) {
            tempValue = logReader.readLine();
            mapList.put(tempKey, tempValue);
        }
        logReader.close();
        if (!mapList.isEmpty()) flush(true);

        File file = new File(log);
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write("");
        fileWriter.flush();
        fileWriter.close();
    }

    /*
     * 文件写入方法
     */
    private void initLogWriter() throws Exception {
        logWriter = new BufferedWriter(new FileWriter(log, true));
    }

    public boolean add(String key, String value) throws Exception {
        boolean flag = isFlush;
        while (flag) {
            flag = isFlush;
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        String msg = LogUtil.buildMsg(key, value);
        logWriter.write(msg);
        logWriter.flush();
        mapList.put(key, value);
        if (mapList.size() >= maxSize) {
            return flush(false);
        }
        return true;
    }

    private boolean flush(boolean force) throws Exception {
        synchronized (MapList.class) {
            isFlush = true;

            try {
                if (force || mapList.size() >= maxSize) {
                    String fileName = mapList.firstKey() + "_" + mapList.lastKey() + ".txt";
                    BufferedWriter tempWriter = new BufferedWriter(new FileWriter(localDir + "/" + fileName));
                    ConcurrentSkipListMap<String, Integer> indexMap = new ConcurrentSkipListMap<>(new Comparator<String>() {
                        public int compare(String s1, String s2) {
                            return s1.compareTo(s2);
                        }
                    });
                    int indexCount = 0;
                    for (String key : mapList.keySet()) {
                        String msg = LogUtil.buildMsg(key, mapList.get(key));
                        tempWriter.write(msg);
                        tempWriter.flush();

                        if (indexCount % indexInterval == 0) {
                            indexMap.put(key, 2 * indexCount);
                        }
                        indexCount++;
                    }
                    tempWriter.newLine();
                    for (String key : indexMap.keySet()) {
                        String msg = LogUtil.buildIdx(key, indexMap.get(key));
                        tempWriter.write(msg);
                        tempWriter.flush();
                    }
                    tempWriter.close();

                    // Configuration conf = new Configuration();
                    // conf.set("fs.default.name", ProcessorImpl.hdfsAddr);
                    // try {
                    //     FileSystem fs = FileSystem.get(conf);
                    //     fs.moveFromLocalFile(new Path(localDir + "/" + fileName), new Path("/"));
                    // } catch (IOException e) {
                    //     e.printStackTrace();
                    // }
                    HadoopUtil.moveFromLocalFile(localDir + "/" + fileName, "/");

                    mapList.clear();
                    logWriter.close();

                    File file = new File(log);
                    FileWriter fileWriter = new FileWriter(file);
                    fileWriter.write("");
                    fileWriter.flush();
                    fileWriter.close();

                    initLogWriter();
                }
                return true;
            } finally {
                isFlush = false;
            }
        }
    }

    /*
     * 文件读取方法
     */
    public String get(String key) throws Exception {
        String value = null;
        if ((value = Cache.get(key)) != null)
            return value;

        for (int i = 0; i < totalKVPodNums; i++) {
            if (i == currentID) {
                continue;
            } else {
                value = MsgUtil.parseMessage(RpcClientFactory.inform(i, MsgUtil.generateMessage("01", key)))[1];
                if (value != null && !"null".equals(value)) {
                    return value;
                }
            }
        }

        if ((value = getFromSSTable(key)) != null)
            return value;
        return null;
    }

    public String getFromMap(String key) throws Exception {
        return mapList.get(key);
    }

    private String getFromSSTable(String key) throws Exception {
        // Configuration conf = new Configuration();
        // conf.set("fs.default.name", ProcessorImpl.hdfsAddr);
        // FileSystem fs = FileSystem.get(conf);

        // FileStatus[] status = fs.listStatus(new Path("/"));
        FileStatus[] status = HadoopUtil.listStatus("/");

        for (int i = 0; i < status.length; i++) {
            String filename = status[i].getPath().getName();
            String[] range = filename.split("\\.")[0].split("_");
            synchronized (readLock) {
                String value = null;
                if ((value = Cache.get(key)) != null)
                    return value;
                if (key.compareTo(range[0]) >= 0 && key.compareTo(range[1]) <= 0) {
                    while (!JobCenter.isAvailable(filename)) {
                        Thread.sleep(10);
                    }
                    File file = new File(localDir + "/" + filename);
                    if (!file.exists() || !file.isFile()) {
                        HadoopUtil.copyToLocalFile(ProcessorImpl.hdfsAddr + "/" + filename, localDir + "/");
                    }

                    value = readSSTable(localDir + "/" + filename, key);

                    if (value != null) {
                        JobCenter.add(filename);
                        return value;
                    }
                }
            }
        }
        return null;
    }


    private String readSSTable(String filename, String key) throws Exception {
        int lineFromIndex = -1;// 索引指向的起始行
        // 从末尾开始往前读
        RandomAccessFile randomAccessFile = new RandomAccessFile(filename, "r");
        long fileLength = randomAccessFile.length();// 文件长度
        long start = randomAccessFile.getFilePointer();// 文件起始指针
        long readIndex = start + fileLength - 2;// 指向文件末尾，由于写的时候一定会在末尾有一个换行符，所以减2
        String line;
        randomAccessFile.seek(readIndex);
        int c;
        while (readIndex > start) {
            c = randomAccessFile.read();
            if (c == '\n' || c == '\r') {
                line = randomAccessFile.readLine();
                if (line == null || line.equals(""))
                    break;
                String[] index = line.split(" ");
                if (!index[0].equals("index"))
                    break;
                if (key.compareTo(index[1]) >= 0) {
                    lineFromIndex = Integer.valueOf(index[2]);
                    break;
                }
                // readIndex--;//??
            }
            readIndex--;
            randomAccessFile.seek(readIndex);
        }
        randomAccessFile.close();

        String value = null;
        if (lineFromIndex != -1) {
            String tempKey = "";
            String tempValue = "";
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));
            for (int i = 0; i < lineFromIndex; i++) {
                tempKey = bufferedReader.readLine();// 跳过前n行
            }
            for (int i = 0; i < 2 * indexInterval; i++) {
                tempKey = bufferedReader.readLine();
                tempValue = bufferedReader.readLine();
                if (tempKey != null && tempValue != null)
                    mapList.put(tempKey, tempValue);
                if (key.equals(tempKey)) {
                    value = tempValue;
                }
            }
            bufferedReader.close();
        }

        return value;
    }
}