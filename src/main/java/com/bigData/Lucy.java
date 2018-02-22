package com.bigData;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcClientFactory;
import cn.helium.kvstore.rpc.RpcServer;

public class Lucy {
	
	int totalKVPodNums;
	int currentID;
	int currentFileNumIn100M;
	String ip;
	String filePath;
	String myDirPathIn100M;
	String otherDirPathIn100M;
	Map<String, Map<String, String>> buffer;
	
	public Lucy() {		
		totalKVPodNums= KvStoreConfig.getServersNum();
		currentID=RpcServer.getRpcServerId();
		currentFileNumIn100M=0;
//		ip="hdfs://114.212.245.119:9000";
//		filePath="/user/hadoop/";
		myDirPathIn100M="/Users/huanghanqian/Downloads/my";
		otherDirPathIn100M="/Users/huanghanqian/Downloads/others";
		ip="hdfs://localhost:8020";
		filePath="/test/";
		buffer=new HashMap<String, Map<String, String>>();
		File dir = new File(myDirPathIn100M);
		if(!dir.exists()){
			dir.mkdir();
		}
		dir = new File(otherDirPathIn100M);
		if(!dir.exists()){
			dir.mkdir();
		}		
	}
	
	public Map<String, String> get(String s) {
		if(buffer.get(s)!=null){
			return buffer.get(s);
		}
		Map<String, String> value=null;
		for(int i=0;i<totalKVPodNums;i++){
    		if(i==currentID){
    			continue;
    		}
    		else{
    			try {
    				value=(Map<String, String>)MessageUtil.parseMessage(RpcClientFactory.inform(i,MessageUtil.generateMessage("01", s)))[1];
					if(value!=null){
						return value;
					}				
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}           
		
    	Configuration conf = new Configuration();
    	conf.set("fs.default.name", ip);
		try {
			FileSystem fs = FileSystem.get(conf);
			Path path = new Path(ip+filePath+s);

			if(fs.exists(path)){
	            FSDataInputStream is = fs.open(path);
	            FileStatus status = fs.getFileStatus(path);
	            byte[] buffer = new byte[Integer.parseInt(String.valueOf(status.getLen()))];
	            is.readFully(0, buffer);
	            is.close();
	            fs.close();
	            String reString = new String(buffer);
	            ObjectMapper mapper = new ObjectMapper();
	            value=mapper.readValue(reString, Map.class);
	        }
		} catch (IOException e) {
			System.out.println("IOException");
			e.printStackTrace();
		}
		return value;
	}
	
	@Deprecated
	public void put(String s, Map<String, String> map){
		buffer.put(s, map);
		try {
			ObjectMapper mapper = new ObjectMapper();
			String json = mapper.writeValueAsString(map);
			int repulicationIndex = (currentID + 1) % totalKVPodNums;
			

			Configuration conf = new Configuration();
			conf.set("fs.default.name", ip);

			FileSystem fs = FileSystem.get(conf);
			// 写文件
			Path path = new Path(ip+filePath + s);
			FSDataOutputStream out = fs.create(path, (short) 1);
			out.writeBytes(json);
			fs.close();
		} catch (IOException e) {
			System.out.println("IOException");
			e.printStackTrace();
		}
	}
	
	public void put2(String s, Map<String, String> map){
		
	}
	
	
	@Deprecated
	public void batchPut(Map<String, Map<String, String>> map) {
		Configuration conf = new Configuration();
    	conf.set("fs.default.name", "hdfs://"+ip);
		try {
			FileSystem fs = FileSystem.get(conf);
			//写文件
			Iterator iter = map.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String key = (String)entry.getKey();
				Map<String, String> value = (Map<String, String>)entry.getValue();
				Path path = new Path(ip+filePath+key);
		        FSDataOutputStream out = fs.create(path,(short)1);
		        ObjectMapper mapper = new ObjectMapper();            
	            String json = mapper.writeValueAsString(value);                
		        out.writeBytes(json);
			}
	        fs.close();
		} catch (IOException e) {
			System.out.println("IOException");
			e.printStackTrace();
		}
    }
	
	public void batchPut2(Map<String, Map<String, String>> map) {
		Configuration conf = new Configuration();
    	conf.set("fs.default.name", ip);
		try {
			FileSystem fs = FileSystem.get(conf);
			Iterator iter = map.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry entry = (Map.Entry) iter.next();
				String key = (String)entry.getKey();
				Map<String, String> value = (Map<String, String>)entry.getValue();			
		        ObjectMapper mapper = new ObjectMapper();            
	            String json = mapper.writeValueAsString(value);   
	            
	            File file=new File(key);
	            FileWriter fileWritter = new FileWriter(file.getName(),false);
	            BufferedWriter bufferWritter = new BufferedWriter(fileWritter);
	            bufferWritter.write(json);
	            bufferWritter.close();
	            	             
	            Path src = new Path(key);
		        Path dst = new Path(ip+filePath);
		        fs.moveFromLocalFile(src, dst);
			}  
			fs.close();
		} catch (IOException e) {
			System.out.println("IOException");
			e.printStackTrace();
		}
    }
	
	public byte[] process(byte[] bytes) {
		Object[] objects=MessageUtil.parseMessage(bytes);
		String type=(String)objects[0];
		if(type.equals("01")){
			String key=(String)objects[1];
			if(buffer.get(key)!=null){
				return MessageUtil.generateMessage("11", buffer.get(key));
			}
		}
        return null;
    }

	public Map<String,String> tt(){
		File file = new File( "/Users/huanghanqian/Downloads/100"); 
		try {  
            if (file.exists()){  
	            ObjectMapper mapper = new ObjectMapper();
	            return  (Map<String, String>)mapper.readValue(file, Map.class);
            }
            else{
            	for(int i=0;i<totalKVPodNums;i++){
            		if(i==currentID){
            			continue;
            		}
            		else{
            			
            		}
            	}
            }
		}catch(IOException e){
			e.printStackTrace();
		}
		return null;
	}
}
