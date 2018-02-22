package com.bigData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

@Deprecated
public class MessageUtil {

    public static Object[] parseMessage(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        Object[] objects = new Object[2];
        String string = new String(bytes);
        String type = string.substring(0, 2);
        objects[0] = type;
        String content = string.substring(2);
        if (type.equals("01")) {
            objects[1] = content;
            return objects;
        } else if (type.equals("11")) {
            Map<String, String> map = new HashMap<String, String>();
            int beginLen = type.getBytes().length;
            int bytes2Len = bytes.length - beginLen;
            byte[] bytes2 = new byte[bytes2Len];
            System.arraycopy(bytes, beginLen, bytes2, 0, bytes2Len);
            try {
                ByteArrayInputStream bi = new ByteArrayInputStream(bytes2);
                ObjectInputStream oi = new ObjectInputStream(bi);
                map = (Map<String, String>) oi.readObject();
                bi.close();
                oi.close();
            } catch (IOException | ClassNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            objects[1] = map;
            return objects;
        } else {
            int index = content.indexOf("&");
            String before = content.substring(0, index);
            String after = string.substring(index + 1);
            int keyLength = Integer.parseInt(before);
            String key = after.substring(0, keyLength);
            String value = after.substring(keyLength);
            Map<String, String> map = new HashMap<String, String>();
            map.put("type", type);
            map.put("key", key);
            map.put("value", value);
            objects[1] = map;
            return objects;
        }
    }

    public static String parseValue(byte[] bytes) {
        return new String(bytes);
    }

    public static byte[] generateMessage(String type, String key, String value) {
        int length = key.length();
        String string = type + length + "&" + key + value;
        return string.getBytes();
    }

    //type为01时，传入key，旨在向其他kvpod获得value
    public static byte[] generateMessage(String type, String message) {
        String string = type + message;
        return string.getBytes();
    }

    //type为11时，传入map，旨在给发送消息的kvpod回应value
    public static byte[] generateMessage(String type, Map<String, String> map) {
        if (map == null) {
            return null;
        }
        byte[] bytes1 = type.getBytes();
        byte[] bytes2 = new byte[]{};
        try {
            ByteArrayOutputStream byt = new ByteArrayOutputStream();
            ObjectOutputStream obj = new ObjectOutputStream(byt);
            obj.writeObject(map);
            bytes2 = byt.toByteArray();
            obj.close();
            byt.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        byte[] unitBytes = new byte[bytes1.length + bytes2.length];
        System.arraycopy(bytes1, 0, unitBytes, 0, bytes1.length);
        System.arraycopy(bytes2, 0, unitBytes, bytes1.length, bytes2.length);
        return unitBytes;
    }

    public void ttt() {

        try {
            ByteArrayInputStream bi = new ByteArrayInputStream(new byte[]{});
            ObjectInputStream oi = new ObjectInputStream(bi);
            Map<String, String> tmp = (Map<String, String>) oi.readObject();
            for (String key : tmp.keySet()) {
                System.out.print(key + "  ");
                System.out.println(tmp.get(key));
            }
            bi.close();
            oi.close();
        } catch (IOException | ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Map<String, String> map = new HashMap<String, String>();
        map.put("age", "12");
        map.put("name", "lucy");
        System.out.println(MessageUtil.parseMessage(MessageUtil.generateMessage("11", map)));
    }

}
