package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * author: winsky
 * date: 2017/11/1
 * description:
 */
public class HDFSTest {
    private static final String HDFS_ADD = "hdfs://114.212.245.119:9000";
    private static final String HDFS_DIR = "/winsky_dir";

    public static void main(String[] args) {
        mkDir();
    }

    private static void mkDir() {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", HDFS_ADD);
        Path path = new Path(HDFS_DIR);
        try {
            FileSystem fs = FileSystem.get(conf);
            if (!fs.isDirectory(path) || !fs.exists(path)) {
                fs.mkdirs(path);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
