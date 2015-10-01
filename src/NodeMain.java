import org.apache.log4j.Logger;

import javax.xml.soap.Node;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by leo on 9/27/15.
 */
public class NodeMain {

    private static int selfPort;
    private static String selfIP;
    private static String nickName;
    private static String discoverIP;
    private static int discoverPort;

    private ServerSocket serverSocket;
    private Socket socket;
    private Socket clientSocket;
    Logger log = Logger.getLogger(NodeMain.class);


    public NodeMain(int selfPort){
        this.selfPort = selfPort;

        try {
            selfIP = InetAddress.getLocalHost().getHostAddress();
            nickName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            System.out.println("Exception occured when tyring to get the host address.");
            e.printStackTrace();
        }

        // create a server socket
        try {
            serverSocket = new ServerSocket(selfPort);
        }catch (IOException e){
            System.out.println("Exception occured when trying to start server");
            e.printStackTrace();
        }


    }

    public void startNode(){

        boolean regSuccess = registerNode();

        if(regSuccess) {

            System.out.println("Node registration successful. Start the server to accept the requests.");
            while (true) {
                try {
                    socket = serverSocket.accept();
                    System.out.println("Node server started...");
                } catch (IOException e) {
                    System.out.println("Exception occured when trying to accept connections");
                    e.printStackTrace();
                }
            }
        }else{
            System.out.println("Cannot start the Node. Exception occured when trying to connect to Discovery node.");
        }

    }


    public static void main(String[] args) {

        if(args.length != 3){
            throw new IllegalArgumentException("Parameter(s): <Self Port> <Discovery Node IP> <Discovery Node Port>" );
        }

        discoverIP = args[1].trim();
        discoverPort = Integer.parseInt(args[2].trim());

        NodeMain nodeMain = new NodeMain(Integer.parseInt(args[0].trim()));
        nodeMain.startNode();

    }


    public boolean registerNode(){

        Socket discoverSocket = null;
        String requestType =null;
        String messRcvd = null;
        String[] messTokens = null;
        boolean regSuccess = false;

        int tempint =0;


        while(!regSuccess) {

            try {
                discoverSocket = getSocket(discoverIP,discoverPort);
            } catch (IOException e) {
                System.out.println("Cannot connect to discovery Node.");
                return regSuccess;
                //e.printStackTrace();
            }

            String identifier = getNodeIdentifier();
            System.out.println("Identifier : "+identifier);

            /*if(tempint==0){
                identifier="1234";
            }*/

            String messToSend = "REG " + selfIP + " " + selfPort + " " + nickName + " " + identifier;

            System.out.println("Message to send to discovery node " + messToSend);
            log.info("Message to send to discovery Node : "+messToSend);

            try {
                sendDataToDestination(discoverSocket, messToSend);
                messRcvd = receiveDataFromDestination(discoverSocket);

            }catch (Exception e){
                System.out.println("Exception occured when trying to send message to discovery node");
                //e.printStackTrace();
                break;
            }finally {
                // Disconnect the socket.
                try {
                    discoverSocket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("Message REceived : " + messRcvd);

            messTokens = messRcvd.split(" ");
            requestType = messTokens[0].trim();
            System.out.println("Request type :"+requestType);

            if (requestType.equals("UNREGFIRST")) {
                System.out.println("Already the server is connected. First unregister and then register.");
                break;
            } else if (requestType.equals("IDCLASH")) {
                System.out.println("Already some other node has generated this id. Please use a different id.");
                tempint++;
                continue;
            }else if(requestType.equals("REGSUCCESS")){
                regSuccess = true;
                System.out.println("Registration successful. come out of loop.");
                break;
            }


        }

        return regSuccess;

    }


    private Socket getSocket(String ipAddress, int port) throws IOException {
        return new Socket(ipAddress,port);
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

    /**
     * This method converts a specified hexadecimal String into a set of bytes.
     *
     * @param hexString
     * @return
     */
    public byte[] convertHexToBytes(String hexString) {
        int size = hexString.length();
        byte[] buf = new byte[size / 2];
        int j = 0;
        for (int i = 0; i < size; i++) {
            String a = hexString.substring(i, i + 2);
            int valA = Integer.parseInt(a, 16);
            i++;
            buf[j] = (byte) valA;
            j++;
        }
        return buf;
    }

    public String getNodeIdentifier(){

        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        return convertBytesToHex(dateFormat.format(date).getBytes());

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
