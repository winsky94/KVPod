package com.bigData;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.util.Map;

/**
 * author: winsky
 * date: 2017/10/29
 * description:
 */
public class MapUtil {
    /**
     * 将map转为String
     *
     * @param map 原始map
     * @return 输入map为空or异常则返回null
     */
    public static String map2String(Map map) throws Exception {
        if (map == null) return null;
        Gson gson = new Gson();
        return gson.toJson(map);
    }

    /**
     * 将String转为Map
     *
     * @param s 原始字符串
     * @return 输入s为空or异常则返回null
     */
    public static Map<String, String> string2Map(String s) throws Exception {
        if (s == null || s.isEmpty()) return null;
        Gson gson = new Gson();
        return gson.fromJson(s, new TypeToken<Map<String, String>>() {
        }.getType());
    }
}
