package com.bigData;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;
import com.bigData.winsky.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class DoubleQueue {

    private static LinkedBlockingQueue<KVPair> addQueue;
    private static LinkedBlockingQueue<Object> writeQueue;
    private static ConcurrentHashMap<String, String> readMap;
    private static Map<String, String> map;

    private static Configuration conf = new Configuration();
    private static FileSystem fs;
    private static Path writePath;

    private int totalKVPodNums;

    public DoubleQueue(int capacity) {
        this.addQueue = new LinkedBlockingQueue<>(capacity);
        this.writeQueue = new LinkedBlockingQueue<>(capacity);
        this.readMap = new ConcurrentHashMap<>();
        // Map<String, String> local = LogUtil.readLog();
        // readMap.putAll(local);


        totalKVPodNums = KvStoreConfig.getServersNum();

        // writePath = new Path("/" + testFile + ".txt");

        conf.set("fs.default.name", ProcessorImpl.hdfsAddr);
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        // conf.set("fs.default.name", "hdfs://172.17.199.45:9000");
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }

        // write();
        map = LogUtil.getLocalFiles();
    }

    public boolean add(String key, String value) throws InterruptedException {
        LogUtil.log(key, value);
        // KVPair kvPair = new KVPair(key, value);
        // addQueue.put(kvPair);
        // writeQueue.take();
        return true;
    }

    private void write() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if (!fs.isFile(writePath)) {
                        fs.create(writePath).close();
                    }

                    FSDataOutputStream fsDataOutputStream = fs.append(writePath);
                    while (true) {
                        KVPair kvPair = addQueue.take();
                        StringBuilder sb = new StringBuilder();
                        sb.append(kvPair.key);
                        sb.append(System.lineSeparator());
                        sb.append(kvPair.value);
                        sb.append(System.lineSeparator());

                        fsDataOutputStream.write(sb.toString().getBytes());
                        fsDataOutputStream.flush();

                        // writeQueue.put(new Object());
                        LogUtil.deleteLog(kvPair.key);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public String get(String key) throws IOException {
        // String value;
        // if ((value = getFromMap(key)) != null) return value;
        //
        // for (int i = 0; i < totalKVPodNums; i++) {
        //     if (i == currentID) {
        //         continue;
        //     } else {
        //         value = MsgUtil.parseMessage(RpcClientFactory.inform(i, MsgUtil.generateMessage("01", key)))[1];
        //         if (value != null && !"null".equals(value)) {
        //             return value;
        //         }
        //     }
        // }
        //
        // return read(key);

        String value = null;
        String path = map.get(key);
        if (path == null) {
            for (int i = 0; i < totalKVPodNums; i++) {
                if (i == RpcServer.getRpcServerId()) {
                    continue;
                } else {
                    value = MsgUtil.parseMessage(RpcClientFactory.inform(i, MsgUtil.generateMessage("01", key)))[1];
                    if (value != null && !"null".equals(value)) {
                        return value;
                    }
                }
            }
            return null;
        } else {
            value = LogUtil.read(path);
        }
        return value;
    }

    public String getFromLocal(String key) throws IOException {
        String path = map.get(key);
        if (path != null) {
            return LogUtil.read(path);
        }
        return null;
    }

    public String getFromMap(String key) {
        return readMap.get(key);
    }

    private String read(String key) throws IOException {
        FileStatus[] status = fs.listStatus(new Path("/"));
        for (int i = 0; i < status.length; i++) {
            String filename = status[i].getPath().getName();
            Path path = new Path("/" + filename);
            InputStream inputStream = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

            String tempKey;
            while ((tempKey = reader.readLine()) != null) {
                if (key.equals(tempKey)) return reader.readLine();
                String tempValue = reader.readLine();
                readMap.put(tempKey, tempValue);
            }
        }
        return null;
    }

    public static void main(String[] args) throws InterruptedException {
        DoubleQueue queue = new DoubleQueue(10000);
        for (int i = 0; i < 100; i++) {
            queue.add("" + i, "111111111");
        }
    }
}


class KVPair {
    public String key;
    public String value;

    public KVPair(String key, String value) {
        this.key = key;
        this.value = value;
    }
}