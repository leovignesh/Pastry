import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by leo on 9/26/15.
 */
public class UnRegisterNode implements  Runnable{

    private Socket socket;
    private String ipAddress;
    private int port;
    private String identifier;
    private Discovery discovery;
    private String nickName;

    public UnRegisterNode(Socket socket,Discovery discovery,String ipAddress,int port,String nickName,String identifier) {
        this.socket = socket;
        this.ipAddress = ipAddress;
        this.port=port;
        this.discovery=discovery;
        this.nickName=nickName;
        this.identifier = identifier;
    }


    @Override
    public void run() {

    	// Remove from the routing table entry and send a message to the node.

    	discovery.nodeDetails.remove(ipAddress);
    	discovery.allIdentifier.remove(identifier);

    	String messToSend = "UNREGSUCCESS";
    	sendDataToDestination(socket, messToSend);

    	System.out.println("UNREGSTER SUCCESSFUL "+identifier+" "+ipAddress+":"+port);

    }
    
    
    private void sendDataToDestination(Socket socket,String messageToSend){

        try {
            int messageLength = messageToSend.trim().length();
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            dataOutputStream.writeInt(messageLength);
            dataOutputStream.write(messageToSend.getBytes(), 0, messageLength);
            dataOutputStream.flush();
        } catch (IOException e){
            System.out.println("Exception occurred when trying to send data to Destination");
            e.printStackTrace();
        }
    }
}
