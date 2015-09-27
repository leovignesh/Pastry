import javax.xml.soap.Node;
import java.net.Socket;
import java.util.*;

/**
 * Created by leo on 9/26/15.
 */
public class RegisterNode implements  Runnable{

    private Socket socket;
    private String ipAddress;
    private int port;
    private Discovery discovery;
    private String nickName;
    private String identifier;

    public RegisterNode(Socket socket,Discovery discovery,String ipAddress,int port,String nickName,String identifier) {

        this.socket = socket;
        this.ipAddress = ipAddress;
        this.port=port;
        this.discovery=discovery;
        this.nickName= nickName;
        this.identifier = identifier;
    }


    @Override
    public void run() {

        // Register a new Node
        NodeDetails nodeDetails = new NodeDetails(ipAddress,port,nickName,identifier);
        String messToSend =null;

        if(discovery.nodeDetails.size()==0){
            System.out.println("First Node in the system.Cannot return any ipaddress");
            messToSend="REGSUCCESS 0";
        }else{

            if(discovery.nodeDetails.get(ipAddress).getIpAddress() !=null){
                System.out.println("Already registerd. Unregister first and then register.");
                messToSend = "UNREGFIRST ";
            }else {

                // Check if the identifier is present for other Ip address.
                if (discovery.nodeDetails.get(ipAddress).getIdentifier() != null) {

                    messToSend = "IDCLASH ";
                } else {

                    messToSend = "REGSUCCESS 1 " + discovery.getRandomIpDetails(1);

                }
            }
        }
        discovery.nodeDetails.put(ipAddress,nodeDetails);

        // send to the Node that requested.



    }

}
