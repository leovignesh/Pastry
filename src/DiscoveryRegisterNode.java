import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by leo on 9/26/15.
 */
public class DiscoveryRegisterNode implements  Runnable{

    private Socket socket;
    private String ipAddress;
    private int port;
    private Discovery discovery;
    private String nickName;
    private String identifier;

    public DiscoveryRegisterNode(Socket socket, Discovery discovery, String ipAddress, int port, String nickName, String identifier) {

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
        DiscoveryNodeDetails discoveryNodeDetails = new DiscoveryNodeDetails(ipAddress,port,nickName,identifier);
        String messToSend =null;

        if(discovery.nodeDetails.size()==0){
            System.out.println("FIRST NODE "+identifier+" "+ipAddress+":"+port);
            messToSend="REGSUCCESS 0";
            discovery.nodeDetails.put(ipAddress, discoveryNodeDetails);
            discovery.allIdentifier.add(identifier);

        }else{

            if(discovery.nodeDetails.get(ipAddress) !=null){
                System.out.println("ALREADY REGISTERED "+identifier);
                messToSend = "UNREGFIRST ";
            }else {

                // Check if the identifier is present for other Ip address.
                if (discovery.allIdentifier.contains(identifier)) {

                    System.out.println("IDCLASH "+identifier+" "+ipAddress+":"+port);
                    messToSend = "IDCLASH ";
                } else {

                    messToSend = "REGSUCCESS 1 " + discovery.getRandomIpDetails(1);
                    discovery.nodeDetails.put(ipAddress, discoveryNodeDetails);
                    discovery.allIdentifier.add(identifier);


                    System.out.println("REGISTRATION SUCCESSFUL : "+identifier);
                    System.out.println("IP ADDRESS : "+ipAddress);
                    System.out.println("PORT : "+port);
                    System.out.println("NICK NAME : "+nickName);

                }
            }
        }


        /* //Testing remove it.
        if(identifier.equals("0239")){

            messToSend = "REGSUCCESS 1 " + "129.82.46.208:4567 frankfort 0200";

        }else if(identifier.equals("1234")){
            messToSend = "REGSUCCESS 1 " + "129.82.46.190:4567 augusta 2000";
        }else if(identifier.equals("3999")){
            messToSend = "REGSUCCESS 1 " + "129.82.46.199:4567 augusta 6000";
        }*/
        // send to the Node that requested.
        sendDataToDestination(socket,messToSend);


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

    private String receiveDataFromDestination(Socket socket){

        byte[] data = null;
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

        return new String(data);
    }

}
