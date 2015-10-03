import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by leo on 10/3/15.
 */
public class JoinOverlay implements  Runnable{

    private String ipAddress;
    private int port;
    private String identifier;

    private Socket peerSocket;

    // Logger Initialization
    Logger log = Logger.getLogger(NodeMain.class);

    public JoinOverlay(String ipAddress, int port,String identifier){
        this.ipAddress = ipAddress;
        this.port = port;
        this.identifier = identifier;
    }

    @Override
    public void run() {
        sendJoinMessage();
    }

    public void sendJoinMessage(){

        try {
            peerSocket = getSocket(ipAddress, port);
        }catch (IOException e){
            log.error("Exception occured when trying to get peer socket.");
        }

        //String messToSend = "JOIN 0 "+

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
