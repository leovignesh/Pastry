import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by leo on 10/11/15.
 */
public class StoreDataServer implements  Runnable{

    // Logger Initialization
    Logger log = Logger.getLogger(StoreDataServer.class);

    private Socket socket = null;

    private String closestIp;
    private int closestPort;
    private String closestIdentifier;


    private Socket nodeSocket;

    public StoreDataServer(Socket socket){
        this.socket = socket;
    }


    @Override
    public void run() {

        byte[] data=null;
        try {
            DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int messageLength = dataInputStream.readInt();
            System.out.println(messageLength);
            data = new byte[messageLength];
            dataInputStream.readFully(data, 0, messageLength);
        }catch (IOException e){
            System.out.println("Exception occured when trying to read message from Input stream");
            e.printStackTrace();
        }

        String messRcvd = new String(data);

        String[] tokens = messRcvd.split(" ");
        String requestType = tokens[0].trim();

        if(requestType.equals("CLOSESTSERVER")){

        	String request = tokens[1].trim();
            closestIp = tokens[2].trim().split(":")[0];
            closestPort = Integer.parseInt(tokens[2].trim().split(":")[1]);
            closestIdentifier = tokens[3].trim();
            String pathTravelled = tokens[4].trim();
            

            log.info("ClosestIP : "+closestIp+" Closest Port : "+closestPort+" Closest Identifier : "+closestIdentifier);

            if(request.equals("FILESAVE")){

                // Send the file to the destination. Enable it after wards.
                sendFileToClosestPlace();
            	

            }else if(request.equals("FILERET")){

                // Send request to the destingation. for se
                // save it locally. and check if it is proper.
            	
            	String fileName = tokens[5].trim();
            	String identifierFileName = tokens[6].trim();
            	String fileFound = tokens[7].trim();
            	
            	if(fileFound.equals("FILEFOUND")){
            	
	            	DataInputStream dataInputStream = null;
	
	                try{
	                    dataInputStream = new DataInputStream(socket.getInputStream());
	                    int messageLength = dataInputStream.readInt();
	                    System.out.println(messageLength);
	                    byte[] data1 = new byte[messageLength];
	                    dataInputStream.readFully(data1, 0, messageLength);
	                    System.out.println("File name received "+fileName);
	                    File newFileName = new File("/s/chopin/b/grad/leovig/Documents/CS555/HW2/receivedFiles"+fileName.substring(fileName.lastIndexOf("/")));
	
	                    FileOutputStream fileOutputStream = new FileOutputStream(newFileName);
	                    fileOutputStream.write(data1);
	                    fileOutputStream.close();
	                    System.out.println("File "+ newFileName +" saved in the location.");
	
	                }catch(IOException e){
	                    System.out.println("Exception occured when trying to get the file.");
	                    e.printStackTrace();
	                }
            	}else if(fileFound.equals("FILENOTFOUND")){
            		
            		System.out.println("File not found at the destination. ");
            		log.info("File not found at the destination. ");
            		
            	}
            	
            	


            }

        }

    }


    

    private void sendFileToClosestPlace(){

    	int fileSize=0; 
    	byte[] fileByte=null;
    	String messToSend="";
    	
    	try{

            File file = new File(StoreDataStart.fileName);
            FileInputStream fileInputStream = new FileInputStream(file);
            fileSize = (int)file.length();
            fileByte = new byte[(int) fileSize];
            int numberRead = fileInputStream.read(fileByte, 0, fileSize);
            
            messToSend = "FILEDATA "+StoreDataStart.fileName+" "+StoreDataStart.hashFileName+" "+fileSize;
            
            // Send the node that a file is coming
            nodeSocket = getSocket(closestIp,closestPort);
            sendDataToDestination(nodeSocket,messToSend);
            
            // Send the actual File.
            
            DataOutputStream dataOutputStream = new DataOutputStream(nodeSocket.getOutputStream());
            dataOutputStream.writeInt(fileSize);
            dataOutputStream.write(fileByte,0,fileSize);
            dataOutputStream.flush();
            
        }catch (IOException e){
        	e.printStackTrace();
            log.error("Exceptin occured when reading from a file.");
        }
    	
    }

    private void sendDataToDestination(Socket socket,String messageToSend) throws IOException{


        int messageLength = messageToSend.trim().length();
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.writeInt(messageLength);
        dataOutputStream.write(messageToSend.getBytes(), 0, messageLength);
        dataOutputStream.flush();

    }

    private String receiveDataFromDestination(Socket socket) throws IOException{


        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
        int messageLength = dataInputStream.readInt();
        System.out.println(messageLength);
        byte[] data = new byte[messageLength];
        dataInputStream.readFully(data, 0, messageLength);

        return new String(data);
    }

    private Socket getSocket(String ipAddress, int port) throws IOException {
        return new Socket(ipAddress,port);
    }


}
