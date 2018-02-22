package com.bigData;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

public class AccessMap {
    private static ConcurrentSkipListMap<String, String> mapList;
    private int maxSize = 10000;//map允许存储最多kv条目
    private int indexInterval = 100;//索引间隔

    private BufferedWriter logWriter;
    private int totalKVPodNums;
    private int currentID;

    private String localDir = "/opt/localdisk/sstable";
    private String log = "/opt/localdisk/log.txt";
    private boolean isFlush = false;

    private static Configuration conf = new Configuration();
    private static FileSystem fs;

    public AccessMap(int max, int interval) {
        totalKVPodNums = KvStoreConfig.getServersNum();
        currentID = RpcServer.getRpcServerId();

        conf.set("fs.default.name", ProcessorImpl.hdfsAddr);
//        conf.set("fs.default.name", "hdfs://172.19.155.200:9000");
        try {
            fs = FileSystem.get(conf);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        this.maxSize = max;
        this.indexInterval = interval;

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

    private void checkLog() {
        try {
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void initLogWriter() {
        try {
            logWriter = new BufferedWriter(new FileWriter(log, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean add(String key, String value) {
        boolean flag = isFlush;
        while (flag) {
            flag = isFlush;
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            String msg = LogUtil.buildMsg(key, value);
            logWriter.write(msg);
            logWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        mapList.put(key, value);
        if (mapList.size() >= maxSize) {
            return flush(false);
        }
        return true;
    }

    private boolean flush(boolean force) {
        synchronized (AccessMap.class) {
            isFlush = true;

            try {
                if (force || mapList.size() >= maxSize) {
                    String fileName = mapList.firstKey() + "_" + mapList.lastKey() + ".txt";
                    Path path = new Path("/" + fileName);

//                    FileSystem fs = FileSystem.get(conf);

                    fs.createNewFile(path);

                    OutputStream outputStream = fs.append(path);
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream),1*1024*1024);

                    ConcurrentSkipListMap<String, Integer> indexMap = new ConcurrentSkipListMap<>(new Comparator<String>() {
                        public int compare(String s1, String s2) {
                            return s1.compareTo(s2);
                        }
                    });

                    StringBuffer dataString = new StringBuffer();
                    StringBuffer tempString = new StringBuffer();
                    int indexCount = 0;
                    for (String key : mapList.keySet()) {
                        if (indexCount % indexInterval == 0) {
                            indexMap.put(key, tempString.length());
                            dataString.append(tempString);
                            tempString = new StringBuffer();
                        }

                        String msg = LogUtil.buildMsg(key, mapList.get(key));
                        tempString.append(msg);
                        indexCount++;
                    }
                    dataString.append(tempString);

                    for (String key : indexMap.keySet()) {
                        String msg = LogUtil.buildIdx(key, indexMap.get(key));
                        writer.write(msg);
                    }
                    writer.newLine();
                    writer.write(dataString.toString());
                    writer.flush();
                    writer.close();

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
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            } finally {
                isFlush = false;
            }
        }
    }

    public String get(String key) throws Exception {
        String value = null;

        if ((value = getFromMap(key)) != null)
            return value;
        // System.out.println("开始读取："+System.currentTimeMillis());
        for (int i = 0; i < totalKVPodNums; i++) {
            if (i == currentID) {
                continue;
            } else {
                value = MsgUtil.parseMessage(RpcClientFactory.inform(i, MsgUtil.generateMessage("01", key)))[1];
                if (value != null && !"null".equals(value)) {
                    // System.out.println("通信完成："+System.currentTimeMillis());
                    return value;
                }
            }
        }
        // System.out.println("通信完成："+System.currentTimeMillis());

        if ((value = getFromSSTable(key)) != null)
            return value;
        return null;
    }

    public String getFromMap(String key) throws Exception {
        return mapList.get(key);
    }

    private String getFromSSTable(String key) throws Exception {
//        FileStatus[] status = HadoopUtil.listStatus("/");
//         System.out.println("开始读取文件："+System.currentTimeMillis());
        FileStatus[] status = fs.listStatus(new Path("/"));
        // System.out.println("列出所有文件："+System.currentTimeMillis());
        String value = null;
        for (int i = 0; i < status.length; i++) {
            String filename = status[i].getPath().getName();
            String[] range = filename.split("\\.")[0].split("_");
            if (key.compareTo(range[0]) >= 0 && key.compareTo(range[1]) <= 0) {
                Path path = new Path("/" + filename);
                InputStream inputStream = fs.open(path);
                // System.out.println("打开文件："+filename+" "+System.currentTimeMillis());
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream),1*1024*1024);

                String indexLine = reader.readLine();
                int index = 0;
                int totalIndex = 0;
                boolean found = false;
                while ((indexLine = reader.readLine()) != null) {
                    String[] strs = indexLine.split(" ");
                    if (!strs[0].equals("index")) {
                        break;
                    }

                    if(found) continue;

                    if(key.compareTo(strs[1]) >= 0) {
                        index = Integer.valueOf(strs[2]);
                        totalIndex += index;
                    } else {
                        found = true;
                    }
                }
                // System.out.println("遍历完索引："+System.currentTimeMillis());

                long a = reader.skip(totalIndex);
                // System.out.println("跳过一定行："+System.currentTimeMillis());

                String tempKey = "";
                String tempValue = "";
                for (int j = 0; j < indexInterval; j++) {
                    tempKey = reader.readLine();
                    tempValue = reader.readLine();
                    if (tempKey != null && tempValue != null)
                        mapList.put(tempKey, tempValue);
                    if (key.equals(tempKey)) {
                        value = tempValue;
                    }
                }
                // System.out.println("将1k条加到内存："+System.currentTimeMillis());
                if (value != null) {
                    return value;
                }
                reader.close();
            }
        }
        // System.out.println("遍历HDFS文件完毕："+System.currentTimeMillis());
        return value;
    }

    public static void main (String args[]) throws Exception {
        AccessMap accessMap = new AccessMap(100, 10);
//        String key = "key";
//        String value = "value";
//        for(int i=0;i<100;i++){
//            String tempKey = key + i;
//            String tempValue = value + i;
//            accessMap.add(tempKey, tempValue);
//        }
        System.out.println(accessMap.get("key20"));

    }
}
