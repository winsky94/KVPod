package com;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

/**
 * author: winsky
 * date: 2017/1/6
 * description: 网络请求工具类
 */
public class HttpUtil {
    private static final int TIMEOUT = 5 * 1000;
    private static final int cache = 10 * 1024;
    private static final String TEMP = ".temp";


    /**
     * @param uri
     * @param param post内容格式为param1=value¶m2=value2¶m3=value3
     * @return
     * @throws Exception
     */
    public static String post(String uri, String param) throws Exception {
        // System.out.println("post请求:" + uri + "," + param);
        InputStream inputStream = null;
        BufferedReader in = null;
        InputStreamReader inputStreamReader = null;
        OutputStream outputStream = null;
        OutputStreamWriter out = null;
        StringBuilder result = new StringBuilder();
        try {
            URL url = new URL(uri);
            URLConnection httpConnection = url.openConnection();
            httpConnection.setConnectTimeout(TIMEOUT);
            httpConnection.setReadTimeout(TIMEOUT);
            httpConnection.setDoOutput(true);
            httpConnection.setUseCaches(false);
            outputStream = httpConnection.getOutputStream();
            out = new OutputStreamWriter(outputStream, "UTF-8");
            out.write(param);
            out.flush();

            inputStream = httpConnection.getInputStream();
            inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
            in = new BufferedReader(inputStreamReader);
            String line;
            while ((line = in.readLine()) != null) {
                result.append(line);
            }
        } finally {
            if (inputStream != null) inputStream.close();
            if (in != null) in.close();
            if (inputStreamReader != null) inputStreamReader.close();
            if (outputStream != null) outputStream.close();
            if (out != null) out.close();
        }
        return result.toString();
    }

    public static String get(String uri, Map<String, String> headers) throws Exception {
        InputStream inputStream = null;
        BufferedReader in = null;
        InputStreamReader inputStreamReader = null;
        StringBuilder result = new StringBuilder();

        try {
            URL url = new URL(uri);
            URLConnection httpConnection = url.openConnection();
            httpConnection.setConnectTimeout(TIMEOUT);
            httpConnection.setReadTimeout(TIMEOUT);
            httpConnection.setDoOutput(true);
            httpConnection.setUseCaches(false);
            if (headers != null) {
                for (String key : headers.keySet()) {
                    httpConnection.addRequestProperty(key, headers.get(key));
                }
            }

            inputStream = httpConnection.getInputStream();
            inputStreamReader = new InputStreamReader(inputStream, "UTF-8");
            in = new BufferedReader(inputStreamReader);
            String line;
            while ((line = in.readLine()) != null) {
                result.append(line);
            }
        } finally {
            if (inputStream != null) inputStream.close();
            if (in != null) in.close();
            if (inputStreamReader != null) inputStreamReader.close();
        }
        return result.toString();
    }
}
