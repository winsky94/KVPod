package com.bigData;

public class MsgUtil {

    public static String[] parseMessage(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        String[] result = new String[2];
        String string = new String(bytes);
        String type = string.substring(0, 2);
        result[0] = type;
        String content = string.substring(2);
        if (content.equals("null") || content.equals("ceived!")) {
            result[1] = null;
        } else {
            result[1] = content;
        }
        return result;
    }

    //type为01时，传入key，旨在向其他kvpod获得value
    //type为11时，传入value，旨在回应其他kvpod我有这个value
    public static byte[] generateMessage(String type, String message) {
        StringBuffer sb = new StringBuffer();
        sb.append(type).append(message);
        String string = sb.toString();
        return string.getBytes();
    }
}
