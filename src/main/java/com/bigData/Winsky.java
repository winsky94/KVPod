package com.bigData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * author: winsky
 * date: 2017/10/29
 * description:
 */
public class Winsky {
    private static String ip = "hdfs://114.212.245.119:9000";
    private static String filePath = "/input/";

    public static void read(String name) {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", ip);
        try {
            FileSystem fs = FileSystem.get(conf);
            Path path = new Path(ip + filePath + name);

            if (fs.exists(path)) {
                FSDataInputStream is = fs.open(path);
                FileStatus status = fs.getFileStatus(path);
                byte[] buffer = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
                is.readFully(0, buffer);
                is.close();
                fs.close();
                String reString = new String(buffer);
                Map<String, String> map = null;
                try {
                    map = MapUtil.string2Map(reString);
                } catch (Exception e) {
                    e.printStackTrace();
                    return;
                }
                System.out.println(map.get("name"));
            }
        } catch (IOException e) {
            System.out.println("IOException");
            e.printStackTrace();
        }
    }

    public static void put(String name, Map<String, String> map) {
        String s = null;
        try {
            s = MapUtil.map2String(map);
        } catch (Exception e) {
            return;
        }

        try {
            Configuration conf = new Configuration();
            conf.set("fs.default.name", ip);

            FileSystem fs = FileSystem.get(conf);
            // 写文件
            Path path = new Path(ip + filePath + name);
            FSDataOutputStream out = fs.create(path, (short) 1);
            out.writeBytes(s);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<>();
        map.put("name", "winsky");
        put("winsky", map);

        read("winsky");
    }
}
