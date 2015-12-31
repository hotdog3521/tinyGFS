package com.client;

public class RID {
	private String chunkHandle = "";
	private int offset = 0;
	private int length = 0;
	public void setChunkHandle(String handle){
		chunkHandle = handle;
	}
	public String getChunkHandle(){
		return chunkHandle;
	}
	
	public void setOffset(int off){
		offset = off;
	}
	public int getOffset(){
		return offset;
	}
	public int getLength(){
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
			
}
