package com.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	int ServerPort = 6789;				//port number of all chunkservers.
	String ServerIP = "localhost";		//IP address to primary chunkserver.
	Socket socket;						//socket connection to chunkserver.
	ObjectOutputStream WriteOutput;		//write stream to chunkserver
	ObjectInputStream ReadInput;		//read stream to chunkserver
	
	public static byte[] RecvPayload(String caller, ObjectInputStream instream, int sz){
		byte[] tmpbuf = new byte[sz];
		byte[] InputBuff = new byte[sz];
		int ReadBytes = 0;
		while (ReadBytes != sz){
			int cntr=-1;
			try {
				cntr = instream.read( tmpbuf, 0, (sz-ReadBytes) );
				for (int j=0; j < cntr; j++){
					InputBuff[ReadBytes+j]=tmpbuf[j];
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.out.println("[Client] RecvPayload: " + caller + " " + instream + " " +sz);
				System.out.println("Error in RecvPayload ("+caller+"), failed to read "+sz+" after reading "+ReadBytes+" bytes.");
				return null;
			}
			if (cntr == -1) {
				System.out.println("Error in RecvPayload ("+caller+"), failed to read "+sz+" bytes.");
				return null;
			}
			else ReadBytes += cntr;
		}
		return InputBuff;
	}
	
	public static int ReadIntFromInputStream(String caller, ObjectInputStream instream){
		int PayloadSize = -1;
		
		byte[] InputBuff = RecvPayload(caller, instream, 4);
		if (InputBuff != null)
			PayloadSize = ByteBuffer.wrap(InputBuff).getInt();
		return PayloadSize;
	}
	
	/**
	 * Creates a new client. Establishes connection with the chunkserver using ServerIP and ServerPort.
	 */
	public Client(){
		if (socket != null) return; //The client is already connected
		try {
			socket = new Socket(ServerIP, ServerPort);
			WriteOutput = new ObjectOutputStream(socket.getOutputStream());
			ReadInput = new ObjectInputStream(socket.getInputStream());	
			
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Send create chunk request to chunkserver. Returns chunkhandle.
	 */
	public String createChunk() {
		try {
			WriteOutput.writeInt(ChunkServer.PAYLOAD_SIZE_LENGTH + ChunkServer.COMMAND_SIZE_LENGTH);
			WriteOutput.writeInt(ChunkServer.CreateChunkCMD);
			WriteOutput.flush();
			
			int ChunkHandleSize =  ReadIntFromInputStream("Client", ReadInput);
			ChunkHandleSize -= ChunkServer.PAYLOAD_SIZE_LENGTH;  //reduce the length by the first four bytes that identify the length
			byte[] CHinBytes = RecvPayload("Client", ReadInput, ChunkHandleSize); 
			return (new String(CHinBytes)).toString();
		} catch (IOException e) {
			System.out.println("Error in Client.createChunk:  Failed to create a chunk.");
			e.printStackTrace();
		} 
		return null;
	}
	
	/**
	 * Send write chunk request to chunkserver. Returns isSuccessful.
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		if(offset + payload.length > ChunkServer.ChunkSize){
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		try {
			byte[] CHinBytes = ChunkHandle.getBytes();
			
			WriteOutput.writeInt(ChunkServer.PAYLOAD_SIZE_LENGTH + ChunkServer.COMMAND_SIZE_LENGTH + (2*4) + payload.length + CHinBytes.length);
			WriteOutput.writeInt(ChunkServer.WriteChunkCMD);
			WriteOutput.writeInt(offset);
			WriteOutput.writeInt(payload.length);
			WriteOutput.write(payload);
			WriteOutput.write(CHinBytes);
			WriteOutput.flush();
			
			int result =  Client.ReadIntFromInputStream("Client", ReadInput);
			if (result == ChunkServer.FALSE) return false;
			return true;
		} catch (IOException e) {
			System.out.println("Error in Client.createChunk:  Failed to create a chunk.");
			e.printStackTrace();
		}
		return false;
	}
	
	/**
	 * Sends a read chunk request to chunkserver. Returns data.
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		
		try {
			byte[] CHinBytes = ChunkHandle.getBytes();
			WriteOutput.writeInt(ChunkServer.PAYLOAD_SIZE_LENGTH + ChunkServer.COMMAND_SIZE_LENGTH + (2*4) + CHinBytes.length);
			WriteOutput.writeInt(ChunkServer.ReadChunkCMD);
			WriteOutput.writeInt(offset);
			WriteOutput.writeInt(NumberOfBytes);
			WriteOutput.write(CHinBytes);
			WriteOutput.flush();
			
			int ChunkSize =  Client.ReadIntFromInputStream("Client", ReadInput);
			ChunkSize -= ChunkServer.PAYLOAD_SIZE_LENGTH;  //reduce the length by the first four bytes that identify the length
			byte[] payload = RecvPayload("Client", ReadInput, ChunkSize); 
			return payload;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}
	
	/*
	 * Debug: start a new client.
	 */
	
	public static void main(String [] args) {
		Client c = new Client();
		c.createChunk();
	}

	


}
