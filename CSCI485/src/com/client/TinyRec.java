package com.client;

public class TinyRec {
	private byte[] payload = null;
	private RID ID = null;
	private int length = 0;
	
	public byte[] getPayload() {
		return payload;
	}
	public void setPayload(byte[] p) {
		this.payload = p;
	}
	
	public RID getRID() {
		return ID;
	}
	public void setRID(RID inputID) {
		ID = inputID;
	}
	
	public int getLength(){
		return length;
	}
	public void setLength(int length) {
		this.length = length;
	}
}
