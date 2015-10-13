import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by leo on 10/11/15.
 */
public class StoreDataClient implements Runnable{

    // Logger Initialization
    Logger log = Logger.getLogger(StoreDataClient.class);

    private Socket socket;


    private String selfIP;
    private int selfPort;
    private String discoveryIP;
    private int discoveryPort;

    private String randomIp;
    private int randomPort;
    private String randomIdentifier;


    public StoreDataClient(String selfIP,int selfPort, String discoveryIP,int discoveryPort){
    	this.selfIP = selfIP;
        this.selfPort = selfPort;
        this.discoveryIP = discoveryIP;
        this.discoveryPort = discoveryPort;

    }

    @Override
    public void run() {
    	while(true){
    		startClient();
    	}
    }


    public void startClient(){

        System.out.println("***********Please enter the option************");
        System.out.println("1, Upload a File to the system :");
        System.out.println("1, Retrieve a File from the system :");


        Scanner scanner = new Scanner(System.in);

        int input =0;

        try{
            do{
                System.out.println("Please enter the Number :");
                while (!scanner.hasNextInt()){
                    scanner.next();
                    System.out.println("Please enter a valid number");
                }
                input = scanner.nextInt();
            }while (input<1 || input>2);
        }catch (Exception e){
            System.out.println("Enter a valid Number");
        }


        switch (input){
            case 1:
                System.out.println("Enter the File Name to be loaded :");
                scanner = new Scanner(System.in);
                
                String fileDetails = scanner.nextLine();
                

                String[] fileNameSplit = fileDetails.split(" ");
                StoreDataStart.fileName = fileNameSplit[0].trim();

                System.out.println("fileNameDetails "+fileDetails);
                
                if(fileNameSplit.length>1){
                    StoreDataStart.hashFileName = fileNameSplit[1].trim();
                }else{
                	StoreDataStart.hashFileName = computeCheckSum();
                }

                System.out.println("hashfilename .."+StoreDataStart.hashFileName);
                getClosestServer("FILESAVE");

                break;

            case 2:
                System.out.println("Enter the File Name to be retrieved :");
                scanner = new Scanner(System.in);

                String fileDetailsToRetrieve = scanner.nextLine();

                String[] fileDetailsRetSplit = fileDetailsToRetrieve.split(" ");
                String fileNameRet = fileDetailsRetSplit[0].trim();

                if(fileDetailsRetSplit.length>1){
                	StoreDataStart.hashFileName = fileDetailsRetSplit[1].trim();
                }

                getClosestServer("FILERET");

                break;

        }

    }

    public String computeCheckSum(){
        //TODO

        return "ABCD";
    }

    private void getRandomIP(){

        String messToSend = "DATASTORE IPADDRESS";
        String messRecvd;
        try {
            socket = getSocket(discoveryIP,discoveryPort);
            sendDataToDestination(socket,messToSend);

            messRecvd = receiveDataFromDestination(socket);

            String[] messageRcvdTokens = messRecvd.split(" ");

            if(messageRcvdTokens[0].trim().equals("RANDOMIP")){

                randomIp = messageRcvdTokens[1].trim().split(":")[0];
                randomPort = Integer.parseInt(messageRcvdTokens[1].trim().split(":")[1]);
                randomIdentifier = messageRcvdTokens[3].trim();
            }

        } catch (IOException e) {
            log.error("Exception occured when trying to get the ip address and port");
            e.printStackTrace();
        }
    }


    private void getClosestServer(String type){

        getRandomIP();
        String messToSend = "GETNODE "+type+" "+StoreDataStart.hashFileName+" "+selfIP+" "+selfPort+" "+"Start=>";
        try {
            socket = getSocket(randomIp,randomPort);
            sendDataToDestination(socket,messToSend);


        } catch (IOException e) {
            log.error("Exception occured when trying to get the ip address and port");
            e.printStackTrace();
        }


    }


    private Socket getSocket(String ipAddress, int port) throws IOException {
        return new Socket(ipAddress,port);
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



}
