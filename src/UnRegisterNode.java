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
    	System.out.println("Routing table size before : "+discovery.nodeDetails.size());
    	System.out.println("Identifier size before : "+discovery.allIdentifier.size());
    	
    	discovery.nodeDetails.remove(ipAddress);
    	discovery.allIdentifier.remove(identifier);
    	
    	System.out.println("Routing table size after : "+discovery.nodeDetails.size());
    	System.out.println("Identifier size after : "+discovery.allIdentifier.size());
    	
    	
    	String messToSend = "UNREGSUCCESS";
    	sendDataToDestination(socket, messToSend);

    	System.out.println("Unregistration successful.");

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
