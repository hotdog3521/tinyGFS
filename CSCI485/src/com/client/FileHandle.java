package com.client;

import java.util.ArrayList;
import java.util.HashMap;

public class FileHandle {
	private ArrayList<String> chunkHandles;
	private String filePath;
	private String primaryIPAddress;
	private HashMap<String, Integer> lastOffset = new HashMap<String, Integer>();
	private int message;
	
	public FileHandle(){
		chunkHandles = new ArrayList<String>();	
	}
	public FileHandle(ArrayList<String> chunkHandles, String primaryIPAddress) {
		this.chunkHandles = chunkHandles;
		this.primaryIPAddress = primaryIPAddress;
	}
	public void addChunkHandle(String chunkHandle) {
		chunkHandles.add(chunkHandle);
	}
	public void setChunkHandles(ArrayList<String> chkHandle){
		chunkHandles = chkHandle;
	}
	public void setLastOffset(String handle, int lastOffset){
		this.lastOffset.put(handle, lastOffset);
	}
	public int getLastOffset(String handle){
		
		return lastOffset.get(handle);
	};
	public void setPrimaryIPAddress(String IPAddress) {
		primaryIPAddress = IPAddress;
	}
	public void setFilePath(String path) {
		filePath = path;
	}
	public String getFilePath() {
		return filePath;
	}
	public ArrayList<String> getChunkHandles() {
		return chunkHandles;
	}
	public String setIPAddress(){
		return primaryIPAddress;
	}
	public int getMessage() {
		return message;
	}
}