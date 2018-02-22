package com.bigData.winsky;

/**
 * author: winsky
 * date: 2017/11/10
 * description:
 */
public class Config {
    /**
     * 节点数据存储文件名
     */
    public static String[] DATA_PATH = new String[]{"/data1", "/data2", "/data3"};
    // public static String[] DATA_PATH = new String[]{"/data0", "/data2", "/data3"};

    /**
     * 节点key存储文件名
     */
    public static String[] KEY_PATH = new String[]{"/key1", "/key2", "/key3"};
    // public static String[] KEY_PATH = new String[]{"/key0", "/key2", "/key3"};
    /**
     * 位置之间的分隔符
     */
    public static String POS_SEPARATOR = "&&&";

    /**
     * key与位置之间的分隔符
     */
    public static String KEY_POS_SEPARATOR = "===";

    /**
     * 写线程读取不到数据休息的时间
     */
    public static final long WRITE_WAIT_TIME = 1;
}
