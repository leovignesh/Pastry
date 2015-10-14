import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by leo on 10/3/15.
 */
public class JoinOverlay implements  Runnable{

    private String peerIPAddress;
    private int peerPort;
    private String peerNickName;
    private String peerIdentifier;
    private NodeDetails selfNodeDetails;


    private Socket peerSocket;

    // Logger Initialization
    Logger log = Logger.getLogger(NodeMain.class);

    public JoinOverlay(NodeDetails selfNodeDetails ,String peerIPAddress, int peerPort,String peerNickName,String peerIdentifier){
        this.selfNodeDetails = selfNodeDetails;
        this.peerIPAddress = peerIPAddress;
        this.peerPort = peerPort;
        this.peerNickName = peerNickName;
        this.peerIdentifier = peerIdentifier;
    }

    @Override
    public void run() {
        sendJoinMessage();
    }

    public void sendJoinMessage(){

        try {
            peerSocket = getSocket(peerIPAddress, peerPort);
        }catch (IOException e){
            e.printStackTrace();
            log.error("Exception occured when trying to get peer socket.");
        }

        String messToSend = "JOIN 0 "+selfNodeDetails.getIpAddress()+" "+selfNodeDetails.getPort()+" "+selfNodeDetails.getNickName()+" "+selfNodeDetails.getIdentifier()+" START";
        try {
            sendDataToDestination(peerSocket,messToSend);
            sendObjectToDestination(peerSocket,NodeMain.routingTable);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Exception occured when trying to send JOIN message to Peer.");
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
    
    private void sendObjectToDestination(Socket socket,Object object) throws IOException{

        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        objectOutputStream.writeObject(object);
        objectOutputStream.flush();

    }

}
