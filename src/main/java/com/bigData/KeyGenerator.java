package com.bigData;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * author: winsky
 * date: 2017/10/29
 * description:
 */
public class KeyGenerator {
    private static char[] origin = "1234567890qwertyuiopasdfghjklzxcvbnm".toCharArray();
    private static int length = origin.length;
    private static Random random = new Random();
    private static final String KEY_FILE = "keys.txt";
    private static final int LEN = 32;

    public static List<String> genKey(int num) {
        return genKey(num, LEN);
    }

    public static List<String> genKey(int num, int len) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < num; i++) {
            System.out.println("生成key:" + i);
            String key = genOneKey(len);
            while (list.contains(key)) {
                key = genOneKey(len);
            }
            boolean append = i != 0;
            write(KEY_FILE, key + System.lineSeparator(), append);
            list.add(key);
        }

        return list;
    }

    private static String genOneKey(int len) {
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < len; i++) {
            key.append(origin[random.nextInt(length)]);
        }
        return key.toString();
    }

    private static void write(String fileName, String content, boolean append) {
        try {
            FileWriter fileWriter = new FileWriter(fileName, append);
            BufferedWriter bufferWriter = new BufferedWriter(fileWriter);
            bufferWriter.write(content);
            bufferWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> getKeyFromFile(int num) {
        List<String> result = new ArrayList<>();
        try {
            FileReader reader = new FileReader(KEY_FILE);
            BufferedReader br = new BufferedReader(reader);
            String str;
            int cnt = 0;
            while ((str = br.readLine()) != null && cnt < num) {
                result.add(str);
            }
            br.close();
            reader.close();
        } catch (IOException ignored) {
        }
        if (num > result.size()) {
            result = genKey(num);
        }
        return result;

    }

    public static void main(String[] args) {
        genKey(10, 32);
    }
}
