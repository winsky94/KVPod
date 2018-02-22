package com.bigData.winsky;

import cn.helium.kvstore.rpc.RpcServer;
import com.bigData.MapUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * author: winsky
 * date: 2017/11/10
 * description:
 */
public class WriteThread extends Thread {
    private String dataPath = null;
    private String keyPath = null;

    private WriteThread() {
    }

    public static void begin() {
        WriteThread thread = new WriteThread();
        thread.start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(Config.WRITE_WAIT_TIME);

                int id = RpcServer.getRpcServerId();
                // id = 0; // for test
                while (!(id > -1 && id < 3)) {
                    Thread.sleep(Config.WRITE_WAIT_TIME);
                    id = RpcServer.getRpcServerId();
                }

                if (dataPath == null) {
                    dataPath = Config.DATA_PATH[id];
                }
                if (keyPath == null) {
                    keyPath = Config.KEY_PATH[id];
                }

                Map<String, Map<String, String>> map;

                synchronized (MyProcessor.mem) {
                    if (MyProcessor.mem.size() != 0) {
                        map = (Map<String, Map<String, String>>) ((HashMap) MyProcessor.mem).clone();
                        MyProcessor.mem.clear();
                    } else {
                        continue;
                    }
                }

                Map<String, String> keys = new HashMap<>();

                HadoopUtil.open(dataPath);
                for (Map.Entry<String, Map<String, String>> entry : map.entrySet()) {
                    String val = MapUtil.map2String(entry.getValue());
                    Long info[] = HadoopUtil.writeData(val);
                    String pos = String.valueOf(info[0]) + Config.POS_SEPARATOR + String.valueOf(info[1]);
                    keys.put(entry.getKey(), pos);
                }
                HadoopUtil.close();

                HadoopUtil.open(keyPath);
                HadoopUtil.writeIndex(keys);
                HadoopUtil.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
