package com.master;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

public class Master implements Runnable {

	protected boolean isStopped = false;
	protected ServerSocket serverSocket = null;
	
	HashMap<String, ArrayList<String>> file_to_chunk = new HashMap<>();
	
	protected int ClientAvailablePorts [];
	protected int ServerAvailablePorts [];
	
	public Master() {
//		file_struct.put("/", new ArrayList<HashMap>());
	}
	
	private synchronized boolean isStopped() {
		return this.isStopped;
	}
	
	public synchronized void stop (){
		this.isStopped = true;
		
		try{
			this.serverSocket.close();
		}catch(IOException e){
			throw new RuntimeException("Error closing server", e);
		}
	}
	
	private int createDir(String src, String dir) {

		return 2;
	}
	
	private int deleteDir(String src, String dir) {


		return 2;
	
	}
	
	private int renameDir(String src, String newname) {

	
		return 2;
	}
	
	private String[] listDir(String src) {
		
		File path = new File("root" + src);
		if(!path.exists()){
			String [] s = new String[1];
			s[0] = "DoesNotExist";
			return s;
		}
		
		
		String[] ret;
		ArrayList<String> dirList = new ArrayList<String>();
		
		mapPermute(path, dirList);
		
	
		ret =  dirList.toArray(new String[dirList.size()]);

		
		System.out.println("Log: Listing directory contents..." + src);
		
		
		return ret;
	}
	public void mapPermute(File path, ArrayList<String> dirList) {
	  

	    // base case
	
		File[] list = path.listFiles();
		if(list.length == 0){
			
		} else {
	        // recursive case
	    	

	        for (File content : list) {
	        	String[] tem = content.getPath().split("root");
	        	
	        	dirList.add(tem[1]);
	            mapPermute(content, dirList);
	        }
	    }
	}
	
	private int createFile(String tgtdir, String filename) {
		File path = new File("root" + tgtdir);
		if(!path.exists()){
			return 0;//
			//return src does not exist
		}
		File[] list = path.listFiles();
		for(int i = 0; i < list.length ; i ++){
			if(list[i].getName().equals(filename)){
				return 1;
			}
		}
		File newFile = new File("root" + tgtdir + filename);
		try {
			newFile.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return 2;
		
	}
	
	private int deleteFile(String tgtdir, String filename) {
		File path = new File("root" + tgtdir);
		if(!path.exists()){
			return 0;//
			//return src does not exist
		}
		File[] list = path.listFiles();
		boolean detected = false;
		for(int i = 0; i < list.length ; i ++){
			if(list[i].getName().equals(filename)){
				detected = true;
			}
		}
		if(!detected){
			return 1;
		}
		File fileToDelete = new File("root" + tgtdir + filename);
		fileToDelete.delete();
	
		
		return 2;
	}
	
	
	private ArrayList<String> openFile(String filepath) {
		 String line = "";
		 int howmanyLine = 0;
		 ArrayList<String> fileList = new ArrayList<String>();
		 FileReader fileReader;
		try {
			fileReader = new FileReader("root" + filepath);
			  // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader = 
                new BufferedReader(fileReader);

            ArrayList<String> tempHandles = new ArrayList<String>();
            ArrayList<String> tempOffsets = new ArrayList<String>();
				while((line = bufferedReader.readLine()) != null) {
				    System.out.println(line);
				    String[] temp = line.split(",");
				    howmanyLine++;
				    tempHandles.add(temp[0]);
				    tempOffsets.add(temp[1]);
				}
				
			
				if(howmanyLine == 0){
					fileList.add("emptyFile");
				}
				fileList.add(tempHandles.size() + "");
				for(int i = 0 ; i < tempHandles.size(); i ++){
					fileList.add(tempHandles.get(i));
				}
				for(int i = 0 ; i < tempHandles.size(); i ++){
					fileList.add(tempOffsets.get(i));
				}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			fileList.add("fileNotFound");
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	
	            
		
	return fileList;
}
	
	private int closeFile(String path, ArrayList<String> chunkHandles, ArrayList<String> offsets){
		
		PrintWriter writer;
		try {
			writer = new PrintWriter("root" + path, "UTF-8");
			for(int i = 0 ; i < chunkHandles.size() ; i ++){
				writer.println(chunkHandles.get(i) + ","  + offsets.get(i));
			}
			

			writer.close();
			return 1;
		} catch (FileNotFoundException | UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			return 0;

		}
		
		
		
	}
	
	private void saveChunks() {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("chunks.txt"));
			oos.writeObject(file_to_chunk);
			oos.flush();
			oos.close();
		} catch(IOException ioe) {
			ioe.printStackTrace();
		} finally {
		}
	}
	
	private void recoverChunks() {
		try {
			ObjectInputStream ois = new ObjectInputStream(new FileInputStream("chunks.txt"));
			file_to_chunk = (HashMap<String, ArrayList<String>>) ois.readObject();
			ois.close();
		} catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
		

	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try {
			serverSocket = new ServerSocket(3000);
			System.out.println("Master running...");
			while(!isStopped())
			{
				
					Socket clientSocket = serverSocket.accept();
					new Thread(new Worker(clientSocket, this)).start();

			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Master stopped");

	  }
	
	
	public static void main (String [] args) throws IOException
	{
		Master master = new Master();
		new Thread(master).start();
	}

// processes client request
class Worker implements Runnable{
	
	
	protected Socket clientSocket;
	protected Master master;
	
	public Worker(Socket clientSocket, Master master)
	{
		this.clientSocket = clientSocket;
		this.master = master;
	}
	// all individual connection processing goes here
	@Override
	public void run() {
		// TODO Auto-generated method stub
		InputStream input = null;
		OutputStream output = null;
		
		
			try {
				input = clientSocket.getInputStream();
				output = clientSocket.getOutputStream();
			
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
			
				
			
			while(!master.isStopped){
				DataInputStream dIn = new DataInputStream(input);
				DataOutputStream dOut = new DataOutputStream(output);
				byte [] standardMessage = null;
				byte [] message = null;
				int messageLength = 0;
				ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
		
				 try {	
					 	 
					 	 int count = 0;
					 	 while(dIn.available() > 0)
					 	 {
					 		if(count > 5)
							{
								break;
							}
							
							standardMessage = new byte[dIn.available()];
							messageLength = standardMessage.length;
							dIn.read(standardMessage);
							outputStream.write(standardMessage);
							count++;
					 	 }
					 	 
					 	message = outputStream.toByteArray();
						messageLength = message.length;
					 	
						 
					 	 if(messageLength > 0)
					 	 {	 
					 		 String str = new String(message, Charset.forName("UTF-8"));
					 		 String command = str.split("-")[0];
					 		 
					 		 
					 		 
					 		 if(command.equals("createDir"))
					 		 {
					 			int resp = createDir(str.split("-")[1], str.split("-")[2]);
						 		
						 		
						 		byte [] b = ByteBuffer.allocate(4).putInt(resp).array();
						 		
						 		dOut.write(b);
						 		dOut.flush();
					 		 }
					 		 
					 		 else if(command.equals("listDir"))
					 		 {
					 			String [] resp = listDir(str.split("-")[1]);
					 			String ans = "";
					 			if(resp.length == 0){
					 				ans = "DoesNotExist";
					 				
					 				byte [] sendable = ans.getBytes(Charset.forName("UTF-8"));
					 				dOut.write(sendable);
					 				dOut.flush();
					 			}else if(!resp[0].equals("DoesNotExist"))
					 			{
					 				for(int i = 0; i < resp.length; i++)
					 				{
					 					ans += resp[i] + "-";
					 				}
					 				
					 				
					 				byte [] sendable = ans.getBytes(Charset.forName("UTF-8"));
					 				dOut.write(sendable);
					 				dOut.flush();
					 			}
					 			
					 			else
					 			{	
					 				ans = resp[0] + "-";
					 				byte [] sendable = ans.getBytes(Charset.forName("UTF-8"));
					 				dOut.write(sendable);
					 				dOut.flush();
					 			}
					 			// very fucking long - convert to string with delimeters "-"
					 			
					 			
						 		
					 		 }
					 		 
					 		 else if(command.equals("deleteDir")){
					 			String src = str.split("-")[1];
					 			String dirToDeleted = str.split("-")[2];
					 			int result = deleteDir(src, dirToDeleted);
					 			byte [] b = ByteBuffer.allocate(4).putInt(result).array();
					 			dOut.write(b);
						 		dOut.flush();
					 		 }
					 		else if(command.equals("renameDir")){
					 			String src = str.split("-")[1];
					 			String newDirName = str.split("-")[2];
					 			int result = renameDir(src, newDirName);
					 			byte [] b = ByteBuffer.allocate(4).putInt(result).array();
					 			dOut.write(b);
						 		dOut.flush();
					 		 }
					 		else if(command.equals("createFile")){
					 			String src = str.split("-")[1];
					 			String newDirName = str.split("-")[2];
					 			int result = createFile(src, newDirName);
					 			byte [] b = ByteBuffer.allocate(4).putInt(result).array();
					 			dOut.write(b);
						 		dOut.flush();
					 		 }
					 		else if(command.equals("deleteFile")){
					 			String targetDir = str.split("-")[1];
					 			String fileName = str.split("-")[2];
					 			int result = deleteFile(targetDir, fileName);
					 			byte [] b = ByteBuffer.allocate(4).putInt(result).array();
					 			dOut.write(b);
						 		dOut.flush();
					 		}else if(command.equals("openFile")){
					 			String filePath = str.split("-")[1];
					 			String ans = "";
					 			ArrayList<String> chunkHandles = openFile(filePath);
					 			for(int i = 0; i < chunkHandles.size(); i++)
				 				{
				 					ans += chunkHandles.get(i) + "-";
				 				}
					 			
					 			byte [] sendable = ans.getBytes(Charset.forName("UTF-8"));
				 				dOut.write(sendable);
				 				dOut.flush();
					 			
					 		}else if(command.equals("closeFile")){
					 			String[] ary = str.split("-");
					 			String path = ary[1];
					 			int length = Integer.parseInt(ary[2]);
					 			ArrayList<String> chunkHandles =  new ArrayList<String>();
					 			ArrayList<String> offsets =  new ArrayList<String>();
					 			for(int i = 3 ; i < length + 3; i ++){
					 				chunkHandles.add(ary[i]);
					 				offsets.add(ary[i + length]);
					 			}
					 			int result = closeFile(path, chunkHandles, offsets);
					 			
					 		
					 			byte [] b = ByteBuffer.allocate(4).putInt(result).array();
					 			dOut.write(b);
						 		dOut.flush();
					 		}
						 }
				 }
						 
				 catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			
				
	        
		}
		
		}
		
}
}
