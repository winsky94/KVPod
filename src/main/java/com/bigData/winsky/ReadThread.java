package com.bigData.winsky;

import cn.helium.kvstore.rpc.RpcServer;
import com.bigData.MapUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author: winsky
 * date: 2017/11/10
 * description:
 */
public class ReadThread extends Thread {
    private String dataPath = null;
    private static List<Map<String, String>> keys = new ArrayList<>();
    private static Map<String, Map<String, String>> data = new HashMap<>();

    static {
        for (int i = 0; i < 3; i++) {
            keys.add(i, new HashMap<>());
        }
    }

    @Override
    public void run() {
        int id = RpcServer.getRpcServerId();
        // id = 0; // for test
        while (!(id > -1 && id < 3)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            id = RpcServer.getRpcServerId();
        }
        if (dataPath == null) {
            dataPath = Config.DATA_PATH[id];
        }
        // 预加载自己存储的数据
        // data.putAll(HadoopUtil.loadLocalData(dataPath, keys.get(id)));
    }

    public static void begin() {
        for (int i = 0; i < Config.KEY_PATH.length; i++) {
            Map<String, String> map = HadoopUtil.readIndex(Config.KEY_PATH[i]);
            keys.set(i, map);
        }
        ReadThread thread = new ReadThread();
        thread.start();
    }

    public static Map<String, String> getValue(String key) {
        return data.get(key);
    }

    public static Map<String, String> readValue(String key) {
        String[] pos = null;
        String path = null;
        for (int i = 0; i < keys.size(); i++) {
            if (keys.get(i).containsKey(key)) {
                pos = keys.get(i).get(key).split(Config.POS_SEPARATOR);
                path = Config.DATA_PATH[i];
                break;
            }
        }
        if (pos != null) {
            try {
                String result = HadoopUtil.readData(path, Long.valueOf(pos[0]), Long.valueOf(pos[1]));
                return MapUtil.string2Map(result);
            } catch (Exception e) {
                return null;
            }
        } else {
            return null;
        }
    }
}
