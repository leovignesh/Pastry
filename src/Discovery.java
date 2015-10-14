import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by leo on 9/26/15.
 */
public class Discovery implements Runnable{

    private Socket socket;

    public static Map<String, DiscoveryNodeDetails> nodeDetails = new ConcurrentHashMap<String, DiscoveryNodeDetails>();
    public static Set<String> allIdentifier = new HashSet<String>();

    public Discovery(Socket socket){
        this.socket = socket;
    }


    @Override
    public void run() {

        byte[] data=null;
        try {
            
        	DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
            int messageLength = dataInputStream.readInt();
            
            data = new byte[messageLength];
            dataInputStream.readFully(data, 0, messageLength);
        }catch (IOException e){
            System.out.println("Exception occured when trying to read message from Input stream");
            e.printStackTrace();
        }

        String messRegistered = new String(data);

        String[] tokens = messRegistered.split(" ");
        String requestType = tokens[0].trim();

        if(requestType.equals("REG")){

            String ipAddress = tokens[1].trim();
            int port = Integer.parseInt(tokens[2].trim());	
            String nickName = tokens[3].trim();
            String identifier=tokens[4].trim();


            DiscoveryRegisterNode discoveryRegisterNode = new DiscoveryRegisterNode(socket,this,ipAddress,port,nickName,identifier);
            Thread thread = new Thread(discoveryRegisterNode);
            thread.start();

        }else if(requestType.equals("UNREG")){

            String ipAddress = tokens[1].trim();
            int port = Integer.parseInt(tokens[2].trim());
            String nickName = tokens[3].trim();
            String identifier = tokens[4].trim();

            UnRegisterNode registerNode = new UnRegisterNode(socket,this,ipAddress,port,nickName,identifier);
            Thread thread = new Thread(registerNode);
            thread.start();
        }else if(requestType.equals("DATASTORE")){

            String type = tokens[1].trim();

            String randomIP = getRandomIpDetails(1);
            String messgeToSend = "RANDOMIP "+randomIP;
            sendDataToDestination(socket,messgeToSend);


        }

    }

    public String getRandomIpDetails(int numberOfIps){

        List<DiscoveryNodeDetails> nodesList= new ArrayList<DiscoveryNodeDetails>(nodeDetails.values());

        // Sort it
        Collections.shuffle(nodesList);
        DiscoveryNodeDetails[] nodeDetail = nodesList.subList(0,numberOfIps).toArray(new DiscoveryNodeDetails[numberOfIps]);

        String messageToSend="";
        for(int i=0;i<nodeDetail.length;i++){
            System.out.println("NOde details IPAddress : "+nodeDetail[i].getIpAddress()+" Port : "+nodeDetail[i].getPort()+" Nick Name :"+nodeDetail[i].getNickName());
            messageToSend = messageToSend+nodeDetail[i].getIpAddress()+":"+nodeDetail[i].getPort()+" "+nodeDetail[i].getNickName()+" "+nodeDetail[i].getIdentifier();
        }

        return messageToSend.trim();

    }

    /**
     * This method converts a set of bytes into a Hexadecimal representation.
     *
     * @param buf
     * @return
     */
    public String convertBytesToHex(byte[] buf) {
        StringBuffer strBuf = new StringBuffer();
        for (int i = 0; i < buf.length; i++) {
            int byteValue = (int) buf[i] & 0xff;
            if (byteValue <= 15) {
                strBuf.append("0");
            }
            strBuf.append(Integer.toString(byteValue, 16));
        }
        return strBuf.toString();
    }


    private String getCurrentTime(){
        return new Timestamp(System.currentTimeMillis()).toString();
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
