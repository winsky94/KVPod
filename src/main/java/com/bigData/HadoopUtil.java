package com.bigData;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 * author: winsky
 * date: 2017/11/1
 * description:Hadoop工具类
 */
public class HadoopUtil {
    private static FileSystem fs = null;

    public static FileSystem getFs() throws Exception {
        if (fs == null) {
            createFs();
        }
        return fs;
    }

    public static boolean closeFs() {
        if (fs != null) {
            try {
                fs.close();
                fs = null;
            } catch (IOException ignored) {
            }
        }
        return fs == null;
    }

    public static boolean moveFromLocalFile(String src, String dst) {
        try {
            fs = getFs();
            fs.moveFromLocalFile(new Path(src), new Path(dst));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean copyToLocalFile(String src, String dst) {
        return copyToLocalFile(src, dst, false);
    }

    public static boolean copyToLocalFile(String src, String dst, boolean checkDst) {
        if (checkDst) {
            File file = new File(dst);
            File parent = file.getParentFile();
            if (!parent.exists() || !parent.isDirectory()) {
                boolean mkDst = parent.mkdirs();
                if (!mkDst) {
                    return false;
                }
            }
        }
        try {
            fs = getFs();
            fs.copyToLocalFile(new Path(src), new Path(dst));
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static FileStatus[] listStatus(String dst) {
        try {
            return fs.listStatus(new Path("/"));
        } catch (IOException e) {
            e.printStackTrace();
            return new FileStatus[]{};
        }
    }

    private static void createFs() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", ProcessorImpl.hdfsAddr);
        fs = FileSystem.get(conf);
    }
}
