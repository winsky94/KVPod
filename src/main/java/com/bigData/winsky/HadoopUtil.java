package com.bigData.winsky;

import cn.helium.kvstore.common.KvStoreConfig;
import com.bigData.MapUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;


public class HadoopUtil {

    private static FileSystem fs = null;
    private static FSDataOutputStream hdfsOutStream = null;

    // public HadoopUtil() {
    static {
        try {
            Configuration conf = new Configuration();
            // String hdfsUrl = "hdfs://172.17.195.115:8020";
            String hdfsUrl = KvStoreConfig.getHdfsUrl();
            conf.set("fs.default.name", hdfsUrl);
            conf.setBoolean("dfs.support.append", true);
            conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
            conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
            fs = FileSystem.get(URI.create(hdfsUrl), conf);
            // fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Long[] writeData(String data) throws Exception {
        long start = hdfsOutStream.getPos();
        hdfsOutStream.write(data.getBytes());
        long end = hdfsOutStream.getPos();
        hdfsOutStream.flush();
        return new Long[]{start, end - start};
    }

    public static void writeIndex(Map<String, String> index) throws Exception {
        BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(hdfsOutStream));
        for (Map.Entry<String, String> entry : index.entrySet()) {
            writer.write(entry.getKey() + Config.KEY_POS_SEPARATOR + entry.getValue());
            writer.newLine();
        }
        writer.close();
    }

    public static String readData(String path, long start, long length) {
        try {
            FSDataInputStream hdfsInStream = fs.open(new Path(path));
            byte[] buffer = new byte[(int) length];
            hdfsInStream.seek(start);
            int len = hdfsInStream.read(buffer);
            hdfsInStream.close();
            return len == (int) length ? new String(buffer) : null;
        } catch (Exception e) {
            return null;
        }
    }

    public static Map<String, String> readIndex(String path) {
        Map<String, String> index = new HashMap<>();
        try {
            if (fs.exists(new Path(path))) {
                FSDataInputStream hdfsInStream = fs.open(new Path(path));
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(hdfsInStream));
                String line;
                String[] parts;
                while ((line = bufferedReader.readLine()) != null) {
                    parts = line.split(Config.KEY_POS_SEPARATOR);
                    if (parts.length == 2) {
                        index.put(parts[0], parts[1]);
                    }
                }
                bufferedReader.close();
                hdfsInStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return index;
    }

    public static Map<String, Map<String, String>> loadLocalData(String path, Map<String, String> index) {
        Map<String, Map<String, String>> data = new HashMap<>();
        try {
            if (fs.exists(new Path(path))) {
                FSDataInputStream fsDataInputStream = fs.open(new Path(path));
                for (Map.Entry<String, String> entry : index.entrySet()) {
                    String[] pos = entry.getValue().split(Config.POS_SEPARATOR);
                    byte[] buffer = new byte[Integer.parseInt(pos[1])];
                    fsDataInputStream.seek(Long.valueOf(pos[0]));
                    int len = fsDataInputStream.read(buffer);
                    if (len == buffer.length) {
                        try {
                            String key = entry.getKey();
                            Map<String, String> val = MapUtil.string2Map(new String(buffer));
                            data.put(key, val);
                        } catch (Exception ignored) {
                        }
                    }
                }
                fsDataInputStream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return data;
    }

    public static void open(String path) throws Exception {
        if (fs.exists(new Path(path))) {
            hdfsOutStream = fs.append(new Path(path));
        } else {
            hdfsOutStream = fs.create(new Path(path));
        }
    }

    public static void close() throws Exception {
        hdfsOutStream.close();
    }
}
