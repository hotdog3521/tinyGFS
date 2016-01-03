package com.client;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.chunkserver.ChunkServer;

public class ClientFS {
	// 3000 for now 
	static int ServerPort = 3000;
	static Socket ClientSocket;
	static ObjectOutputStream WriteOutput;
	static ObjectInputStream ReadInput;
	
	public ClientFS(){
		if (ClientSocket != null) return;
		//The client is already connected
//			BufferedReader binput = new BufferedReader(new FileReader(ChunkServer.ClientConfigFile));
//			String port = binput.readLine();
//			port = port.substring( port.indexOf(':')+1 );
//			ServerPort = Integer.parseInt(port);
			try{
				ClientSocket = new Socket("localhost", 3000);
				
			} catch(IOException ioe) {
				ioe.printStackTrace();
			}
			
			
//			WriteOutput = new ObjectOutputStream(ClientSocket.getOutputStream());
//			ReadInput = new ObjectInputStream(ClientSocket.getInputStream());
		}
	
	
	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail //Returned when a method fails
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485", "Lecture1")
	 * @throws IOException 
	 */
	public FSReturnVals CreateDir(String src, String dirname){
		
		
		
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try {
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		// add dashes to separate the message
		String createDirCMD = "createDir-";
		src += "-";
		
		byte[] CMD = createDirCMD.getBytes();
		byte[] srcBytes = src.getBytes();
		byte[] dirnameBytes = dirname.getBytes();
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try {
			outputStream.write(CMD);
			outputStream.write(srcBytes);
			outputStream.write(dirnameBytes);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		try{
			
			
			byte [] message = outputStream.toByteArray();
			dos.write(message);
			dos.flush();
			
			
			int result = -1;
			while(result == -1)
			{
				if(dis.available() > 0)
				{
					
					byte [] b = new byte[4];
					dis.read(b);
					result = ByteBuffer.wrap(b).getInt();
					
					// 0 == SrcDirNotExistent
					// 1 == DestDirExists
					// 2 == success
					switch(result){
					case 0:
						return FSReturnVals.SrcDirNotExistent;
					case 1:
						return FSReturnVals.DestDirExists;
					case 2:
						return FSReturnVals.Success;
					
					}
				}
			}
			
		}catch(Exception e){
			
		}
	
		return null;
	}
	
public String[] ListDir(String tgt) {
		
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try {
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		String listDirCMD = "listDir-";
		tgt += "-";
		byte[] CMD = listDirCMD.getBytes();
		byte[] targetSrc = tgt.getBytes();

		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try {
			outputStream.write(CMD);
			outputStream.write(targetSrc);
			
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		byte [] message = outputStream.toByteArray();
		
		try{
			dos.write(message);
			dos.flush();
			
			boolean accepted = false;
			byte [] fullList = null;

			while(!accepted)
			{
				int count = 0;
				
				outputStream = new ByteArrayOutputStream( );
				byte [] standardMessage = null;
				while(dis.available() > 0)
				{
					if(count > 10)
					{
						break;
					}
					
					standardMessage = new byte[dis.available()];
					dis.read(standardMessage);
					outputStream.write(standardMessage);
					count++;
					accepted = true;
				}

			}
				fullList = outputStream.toByteArray();
				String str = new String(fullList, Charset.forName("UTF-8"));
				
				int result;
				String[] returnArray = str.split("-");
				
				if(returnArray[0].equals("DoesNotExist"))
				{
					result = 0;
				}
				
				else
				{
					result = 1;
				}
				
				switch(result){
				case 0: // directory does not exist
					
				
					returnArray = new String[1];
					returnArray[0] = FSReturnVals.SrcDirNotExistent.toString();
					return returnArray;
				case 1: // directory exists
					System.out.println("returns Array");
					return returnArray;
				case 2:
					return null;
				}
			}
		
		catch(Exception e){
			
		}
		
		return null;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485", "Lecture1")
	 * @throws IOException 
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try{
	
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
			
			}catch(Exception e){
			
		}
	
		
		String deleteDirCMD = "deleteDir-";

		src += "-";
		
		byte[] CMD = deleteDirCMD.getBytes();
		byte[] srcBytes = src.getBytes();
		byte[] dirnameBytes = dirname.getBytes();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try{
		
			outputStream.write(CMD);
			outputStream.write(srcBytes);
			outputStream.write(dirnameBytes);
			byte [] message = outputStream.toByteArray();
			dos.write(message);
			dos.flush();
			
			
			int result = -1;
			while(result == -1)
			{
				if(dis.available() > 0)
				{
					
					byte [] b = new byte[4];
					dis.read(b);
					result = ByteBuffer.wrap(b).getInt();
					
					// 0 == SrcDirNotExistent
					// 1 == DestDirExists
					// 2 == success
					switch(result){
					case 0:
						return FSReturnVals.SrcDirNotExistent;
					case 1:
						return FSReturnVals.DirNotEmpty;
					case 2:
						return FSReturnVals.Success;
					
					}
				}
			}		
				
			
		
		}catch(Exception e){
			
		}
			
		return null;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try{
	
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
			
			}catch(Exception e){
			
		}

		String renameDirCMD = "renameDir-";

		src += "-";
	
		byte[] CMD = renameDirCMD.getBytes();
		byte[] srcBytes = src.getBytes();
		byte[] newNameBytes = NewName.getBytes();
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try{
		
			outputStream.write(CMD);
			outputStream.write(srcBytes);
			outputStream.write(newNameBytes);
			byte [] message = outputStream.toByteArray();
			dos.write(message);
			dos.flush();
			
			
			int result = -1;
			while(result == -1)
			{
				if(dis.available() > 0)
				{
					
					byte [] b = new byte[4];
					dis.read(b);
					result = ByteBuffer.wrap(b).getInt();
					
					// 0 == SrcDirNotExistent
					// 1 == DestDirExists
					// 2 == success
					switch(result){
					case 0:
						return FSReturnVals.SrcDirNotExistent;
					case 1:
						return FSReturnVals.DirNotEmpty;
					case 2:
						return FSReturnVals.Success;
					
					}
				}
			}		
				
			
		
		}catch(Exception e){
			
		}
		
		return null;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 *xample usage: ListDir("/Shahram/CSCI485")
	 */
	

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try{
	
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
			
			}catch(Exception e){
			
		}

		

		tgtdir += "-";
	
		String createFileCMD = "createFile-";
		byte[] CMD = createFileCMD.getBytes();
		byte[] targetDirBytes = tgtdir.getBytes();
		byte[] fileNameBytes = filename.getBytes();
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try{
		
			outputStream.write(CMD);
			outputStream.write(targetDirBytes);
			outputStream.write(fileNameBytes);
			byte [] message = outputStream.toByteArray();
			dos.write(message);
			dos.flush();
			
			
			int result = -1;
			while(result == -1)
			{
				if(dis.available() > 0)
				{
					
					byte [] b = new byte[4];
					dis.read(b);
					result = ByteBuffer.wrap(b).getInt();
					
					// 0 == SrcDirNotExistent
					// 1 == DestDirExists
					// 2 == success
					switch(result){
					case 0:
						return FSReturnVals.SrcDirNotExistent;
					case 1:
						return FSReturnVals.FileExists;
					case 2:
						return FSReturnVals.Success;
													
					}
				}
			}		
				
			
		
		}catch(Exception e){
			
		}
		
		return null;
		
		
		
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try{
	
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
			
			}catch(Exception e){
			
		}

		

		tgtdir += "-";
	
		String deleteFileCMD = "deleteFile-";
		byte[] CMD = deleteFileCMD.getBytes();
		byte[] targetDirBytes = tgtdir.getBytes();
		byte[] fileNameBytes = filename.getBytes();
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try{
		
			outputStream.write(CMD);
			outputStream.write(targetDirBytes);
			outputStream.write(fileNameBytes);
			byte [] message = outputStream.toByteArray();
			dos.write(message);
			dos.flush();
			
			
			int result = -1;
			while(result == -1)
			{
				if(dis.available() > 0)
				{
					
					byte [] b = new byte[4];
					dis.read(b);
					result = ByteBuffer.wrap(b).getInt();
					
					// 0 == SrcDirNotExistent
					// 1 == DestDirExists
					// 2 == success
					switch(result){
					case 0:
						return FSReturnVals.SrcDirNotExistent;
					case 1:
						return FSReturnVals.FileDoesNotExist;
					case 2:
						return FSReturnVals.Success;
													
					}
				}
			}		
				
			
		
		}catch(Exception e){
			
		}
		
		return null;
		
		
		
		
		
		
		
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx")
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {

		
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try{
	
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
			
			}catch(Exception e){
			
		}

		

		FilePath += "-";
	
		String deleteFileCMD = "openFile-";
		byte[] CMD = deleteFileCMD.getBytes();
		byte[] filePath = FilePath.getBytes();

		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try{
		
			outputStream.write(CMD);
			outputStream.write(filePath);
			byte [] message = outputStream.toByteArray();
			dos.write(message);
			dos.flush();
			
			boolean accepted = false;
			byte [] fullList = null;
			while(!accepted)
			{
				int count = 0;
				
				outputStream = new ByteArrayOutputStream( );
				byte [] standardMessage = null;
				while(dis.available() > 0)
				{
					if(count > 10)
					{
						break;
					}
					
					standardMessage = new byte[dis.available()];
					dis.read(standardMessage);
					outputStream.write(standardMessage);
					count++;
					accepted = true;
				}

			}
				fullList = outputStream.toByteArray();
				String str = new String(fullList, Charset.forName("UTF-8"));
				
				int result;
				String[] returnArray = str.split("-");
				
				if(returnArray[0].equals("fileNotExist"))
				{
					result = 0;
				}
				
				else
				{
					result = 1;
					//success
					ofh.setFilePath(FilePath);
					if(returnArray[0].equals("emptyFile")){
						ArrayList<String> handlesInArrayList = new ArrayList<String>();
						ofh.setChunkHandles(handlesInArrayList);
					}else{
						int sizeOfChunkLists = Integer.parseInt(returnArray[0]);
						ArrayList<String> handlesInArrayList = new ArrayList<String>();
			
						for(int i = 0 ; i < sizeOfChunkLists; i ++){
							handlesInArrayList.add(returnArray[1 + i]);
							ofh.setLastOffset(returnArray[1 + i], Integer.parseInt(returnArray[1 + i + sizeOfChunkLists]));
						}

						ofh.setChunkHandles(handlesInArrayList);
						
					}
				}
				
				switch(result){
				case 0:
					return FSReturnVals.SrcDirNotExistent;
				case 1:
					return FSReturnVals.Success;
				}
		
	
		
	
				
			
		
		}catch(Exception e){
			
		}
		
		return null;
		
	
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		DataOutputStream dos = null;
		DataInputStream dis = null;
		try{
	
			dos = new DataOutputStream(ClientSocket.getOutputStream());
			dis = new DataInputStream(ClientSocket.getInputStream());
			
			}catch(Exception e){
			
		}

		

		String filePath = ofh.getFilePath();
		
		ArrayList<String> list = ofh.getChunkHandles();
		ArrayList<Integer> offset = new ArrayList<Integer>();
		String sizeOfList = Integer.toString(list.size()) + "-";
		String chunkHandleList = "";
		String offsetList = "";
		for(int i = 0 ; i < list.size(); i ++){
			offset.add(ofh.getLastOffset(list.get(i)));
			chunkHandleList += (list.get(i) + "-");
			offsetList += (offset.get(i) + "-");
		}
		String deleteFileCMD = "closeFile-";
		byte[] CMD = deleteFileCMD.getBytes();
		byte[] targetDirBytes = filePath.getBytes();
		byte[] size = sizeOfList.getBytes();
		byte[] chunkHandleListByte = chunkHandleList.getBytes();
		byte[] offsetListByte = offsetList.getBytes();
		
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		try{
		
			outputStream.write(CMD);
			outputStream.write(targetDirBytes);
			outputStream.write(size);
			outputStream.write(chunkHandleListByte);
			outputStream.write(offsetListByte);
			byte [] message = outputStream.toByteArray();
			dos.write(message);
			dos.flush();
			
			
			int result = -1;
			while(result == -1)
			{
				if(dis.available() > 0)
				{
					
					byte [] b = new byte[4];
					dis.read(b);
					result = ByteBuffer.wrap(b).getInt();
					
					// 0 == SrcDirNotExistent
					// 1 == DestDirExists
					// 2 == success
					switch(result){
					case 0:
						return FSReturnVals.BadHandle;
					case 1:
						return FSReturnVals.Success;
													
					}
				}
			}		
				
			
		
		}catch(Exception e){
			
		}
		
		return null;
		
	}
	
	


}