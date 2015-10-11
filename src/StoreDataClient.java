import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Scanner;
import java.util.logging.FileHandler;

/**
 * Created by leo on 10/11/15.
 */
public class StoreDataClient implements Runnable{

    // Logger Initialization
    Logger log = Logger.getLogger(StoreDataClient.class);

    private Socket socket;


    private int selfPort;
    private String discoveryIP;
    private int discoveryPort;

    private String randomIp;
    private int randomPort;
    private String randomIdentifier;

    private String selfFileIdentifier;

    public StoreDataClient(int selfPort, String discoveryIP,int discoveryPort){

        this.selfPort = selfPort;
        this.discoveryIP = discoveryIP;
        this.discoveryPort = discoveryPort;

    }

    @Override
    public void run() {
        startClient();
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
                System.out.println("Enter the File to be loaded :");
                scanner = new Scanner(System.in);
                String fileDetails = scanner.nextLine();

                String[] fileNameSplit = fileDetails.split(" ");
                String fileName = fileNameSplit[0].trim();

                if(fileNameSplit.length>2){
                    selfFileIdentifier = fileNameSplit[1].trim();
                }

                if(selfFileIdentifier==null){
                    selfFileIdentifier = computeCheckSum();
                }

                getClosestServer();

                try{

                    File file = new File(fileName);
                    FileInputStream fileInputStream = new FileInputStream(file);
                    long fileSize = file.length();


                }catch (IOException e){
                    log.error("Exceptin occured when reading from a file.");
                    System.out.println();
                }

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


    private void getClosestServer(){

        String messToSend = "GETNODE "+selfFileIdentifier;
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
