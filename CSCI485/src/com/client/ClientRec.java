package com.client;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	static int ServerPort = 3011;
	static Socket ClientSocket;
	static ObjectOutputStream WriteOutput;
	static ObjectInputStream ReadInput;
	private Client client = new Client();
	public ClientRec(){

			
	}
	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		if(payload.length + 4 > ChunkServer.ChunkSize){
			return FSReturnVals.RecordTooLong;
		}
		if(!RecordID.getChunkHandle().equals("")){
			return FSReturnVals.BadRecID;
		}
		ArrayList<String> handles = ofh.getChunkHandles();
		String newChunkHandle = "";
		boolean writeSuccess = false;
		if(handles.size() == 0){
			//empty file so we should make new chunk
			newChunkHandle = client.createChunk();
			ofh.addChunkHandle(newChunkHandle);
			ofh.setLastOffset(newChunkHandle, payload.length + 4);
			writeSuccess = client.writeChunk(newChunkHandle, payload, 0);
		}else{
			newChunkHandle = handles.get(handles.size() - 1);
			int lastOffset = ofh.getLastOffset(newChunkHandle);
			// check it exceed 
		
			if((payload.length + lastOffset + 4) > ChunkServer.ChunkSize){
				//exceed
				newChunkHandle = client.createChunk();
				ofh.addChunkHandle(newChunkHandle);
				ofh.setLastOffset(newChunkHandle, payload.length);
				writeSuccess = client.writeChunk(newChunkHandle, payload, 0);
			}else{
				
				ofh.setLastOffset(newChunkHandle, lastOffset + payload.length + 4);
				writeSuccess = client.writeChunk(newChunkHandle, payload, lastOffset);
			}
		}
		
		if(handles == null){
			return FSReturnVals.BadHandle;
		}
		
		
	
		if(!writeSuccess){
			return null;
		}else{
			RecordID.setChunkHandle(newChunkHandle);
			RecordID.setOffset(ofh.getLastOffset(newChunkHandle));
			return FSReturnVals.Success;
		}
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		ArrayList<String> handles = ofh.getChunkHandles();
		if(handles.size() == 0){
			return FSReturnVals.BadHandle;
		}
		int length = RecordID.getLength();
		byte[] deleted = new byte[length];
		for(int i = 0 ; i < length ; i ++){
			deleted[i] = 33;
		}
			
		
		boolean result = client.writeChunk(RecordID.getChunkHandle(), deleted, RecordID.getOffset());
	
		if(result == true){
			return FSReturnVals.Success;
		}
		return null;
		
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		ArrayList<String> handles = ofh.getChunkHandles();
		if(handles.size() == 0){
			return FSReturnVals.BadHandle;
		}
		String firstChunkHandle = handles.get(0);
		byte[] result = client.readChunk(firstChunkHandle, 0, 0);
		String resultString = new String(result, Charset.forName("UTF-8"));
		if(resultString.equals("0")){
			return FSReturnVals.RecDoesNotExist;
		}
		rec.setPayload(result);
		RID rid = new RID();
		rid.setChunkHandle(firstChunkHandle);
		rid.setOffset(0);
		rid.setLength(result.length);
		rec.setRID(rid);
		rec.setLength(result.length + 4);
		return FSReturnVals.Success;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		return null;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		String previousChunkHandle = pivot.getChunkHandle();
		int previousOffset = pivot.getOffset();

		ArrayList<String> handles = ofh.getChunkHandles();
		String nextChunkHandle = "";
		boolean nextChunkHandleExists = false;
		for(int i = 0 ; i < handles.size();  i++){
			if(handles.get(i).equals(previousChunkHandle)){
				nextChunkHandle = handles.get(i +1);
				nextChunkHandleExists =true;
				break;
			}
		}
		if(handles.size() == 0){
			return FSReturnVals.BadHandle;
		}
		byte[] result = client.readChunk(previousChunkHandle, previousOffset, 1);
		String resultString = new String(result, Charset.forName("UTF-8"));
		byte[] newResult;
		if(resultString.equals("0")){
			return FSReturnVals.RecDoesNotExist;
		}else if(resultString.equals("1")){
			//get new chunk Handle 
			if(nextChunkHandleExists){
				newResult = client.readChunk(nextChunkHandle, 0, 0);
				rec.setPayload(newResult);
				RID rid = new RID();
				rid.setChunkHandle(nextChunkHandle);
				rid.setOffset(0);
				rec.setRID(rid);
				rid.setLength(newResult.length);
				return FSReturnVals.Success;
				
			}
			
		}
		rec.setPayload(result);
		RID rid = new RID();
		rid.setChunkHandle(previousChunkHandle);
		rid.setOffset(previousOffset + 4 + pivot.getLength());
		rid.setLength(result.length);
		rec.setRID(rid);
		return FSReturnVals.Success;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		return null;
	}

}
