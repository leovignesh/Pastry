import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
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

            closestIp = tokens[1].trim().split(":")[0];
            closestPort = Integer.parseInt(tokens[1].trim().split(":")[1]);
            closestIdentifier = tokens[2].trim();

            log.info("ClosestIP : "+closestIp+" Closest Port : "+closestPort+" Closest Identifier : "+closestIdentifier);

            // Send the file to the destination. Enable it after wards.
            //sendFileToClosestPlace();

        }

    }


    private void sendFileToClosestPlace(){

        String messToSend = "PLACEFILE";

        try {
            nodeSocket = getSocket(closestIp,closestPort);
            sendDataToDestination(nodeSocket,messToSend);
            // Send the actual File.

        } catch (IOException e) {
            log.error("Exception occured when getting closest IP and Port");
            e.printStackTrace();
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
