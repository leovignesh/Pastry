import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Created by leo on 9/27/15.
 */
public class NodeServer implements  Runnable{

    private Socket socket;
    private NodeDetails selfNodeDetails;
    private NodeMain nodeMain;

    // Logger Initialization
    Logger log = Logger.getLogger(NodeMain.class);

    public NodeServer(Socket socket,NodeDetails selfNodeDetails,NodeMain nodeMain) {
        this.socket = socket;
        this.selfNodeDetails = selfNodeDetails;
        this.nodeMain = nodeMain;
    }

    @Override
    public void run() {

        log.info("Server Connected.....");
        log.debug("Thread Name : " + Thread.currentThread().getName());

        String mssRcvd=null;
        try {
            mssRcvd = receiveDataFromDestination(socket);
        }catch (IOException e){

        }
        log.debug("Request received :  " + mssRcvd);
        String[] tokens = mssRcvd.split(" ");

        String requestType = tokens[0].trim();

        if(requestType.equals("JOIN")){
            String newIPRec = tokens[2].trim();
            int newPortRec = Integer.parseInt(tokens[3].trim());
            String newNickNameRec = tokens[4].trim();
            String newIdentifierRec = tokens[5].trim();

            // Check the leafset
            if(nodeMain.leafLeft == null){
                log.info("First Guy so no leaf nodes. So setting it up.");

                nodeMain.leafLeft = new NodeDetails(newIPRec,newPortRec,newIdentifierRec,newNickNameRec);
                nodeMain.leafRight = new NodeDetails(newIPRec,newPortRec,newIdentifierRec,newNickNameRec);

            }else{
                String tempLeftIdentifier = nodeMain.leafLeft.getIdentifier();
                String tempRightIdentifier = nodeMain.leafRight.getIdentifier();

                int tempIntLeftIdentifier = Integer.parseInt(tempLeftIdentifier,16);
                int tempIntRightIdentifier = Integer.parseInt(tempRightIdentifier,16);

                log.info("Left id "+tempIntLeftIdentifier);
                log.info("Righ id "+tempIntRightIdentifier);

                //isBetween(tempIntLeftIdentifier,)
            }
        }


    }

    private boolean isBetween(int firstValue,int secondValue,int compareValue){

        if(firstValue<secondValue){

            if(compareValue> firstValue  && compareValue<=secondValue){
                return true;
            }
        }else {
            if(compareValue>firstValue || compareValue<=secondValue){
                return true;
            }
        }
        return false;
    }

    private void sendDataToDestination(Socket socket,String messageToSend) throws IOException {


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
