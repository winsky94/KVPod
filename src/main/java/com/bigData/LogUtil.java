package com.bigData;

/**
 * author: winsky
 * date: 2017/11/2
 * description:
 */
public class LogUtil {
    public static String buildMsg(String key, String value) {
        StringBuffer sb = new StringBuffer();
        sb.append(key);
        sb.append(System.lineSeparator());
        sb.append(value);
        sb.append(System.lineSeparator());
        return sb.toString();
    }

    public static String buildIdx(String key, int value) {
        StringBuffer sb = new StringBuffer();
        sb.append("index ");
        sb.append(key);
        sb.append(" ");
        sb.append(value);
        sb.append(System.lineSeparator());
        return sb.toString();
    }
}
