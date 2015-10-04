import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by leo on 9/27/15.
 */
public class NodeMain {

    // self Node details
    private static int selfPort;
    private static String selfIP;
    private static String selfIdentifier=null;
    private static String selfnickName;

    // Random Node details
    private static int randomNodePort;
    private static String randomNodeIP;
    private static String randomNodeIdentifier;
    private static String randomNodeNickName;
    public static int randomNodeID=0;

    // Discovery Node details
    private static String discoverIP;
    private static int discoverPort;

    // Socket details
    private ServerSocket serverSocket;
    private Socket socket;
    private Socket clientSocket;

    // Logger Initialization
    Logger log = Logger.getLogger(NodeMain.class);

    // Datastructues
    public static Map<Integer,ArrayList<NodeDetails>> routingTable = new ConcurrentHashMap<Integer,ArrayList<NodeDetails>>();


    // Leafset
    public NodeDetails leafLeft = null;
    public NodeDetails leafRight = null;



    public NodeMain(int selfPort){
        this.selfPort = selfPort;

        try {
            selfIP = InetAddress.getLocalHost().getHostAddress();
            selfnickName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            log.error("Exception occured when tyring to get the host address.");
            e.printStackTrace();
        }

        // create a server socket
        try {
            serverSocket = new ServerSocket(selfPort);
        }catch (IOException e){
            log.error("Exception occured when trying to start server");
            e.printStackTrace();
        }


    }

    public void startNode(){

        boolean regSuccess = registerNode();

        if(regSuccess) {

            // Build routing table
            NodeDetails selfnodeDetails = new NodeDetails(selfIP,selfPort,selfIdentifier,selfnickName);

            InitialRoutingTable selfRoute = new InitialRoutingTable(selfnodeDetails,this);
            selfRoute.buildInitialRoutingTable();


             // Start the client for giving input
            NodeClient nodeClient = new NodeClient(this);
            Thread threadClient = new Thread(nodeClient);
            threadClient.start();



            System.out.println("Node registration successful. Join the overlay if you are not the first Node.");

            // Join the overlay if you are not the first guy.
            if(randomNodeID!=0) {
                JoinOverlay joinOverlay = new JoinOverlay(selfnodeDetails, randomNodeIP, randomNodePort, randomNodeNickName,randomNodeIdentifier);
                Thread thread = new Thread(joinOverlay);
                thread.start();
            }else{


            }

            while (true) {
                try {
                    socket = serverSocket.accept();
                    System.out.println("Node server started...");

                    NodeServer nodeServer = new NodeServer(socket,selfnodeDetails,this);
                    Thread thread1 = new Thread(nodeServer);
                    thread1.start();

                } catch (IOException e) {
                    log.error("Exception occured when trying to accept connections");
                    e.printStackTrace();
                }
            }
        }else{
            log.error("Cannot start the Node. Exception occured when trying to connect to Discovery node.");
        }

    }


    public void buildLeafSet(){
        if(randomNodeID==0){
            log.info("First node so no leafset");
        }else{
            log.info("Build the leaf set ");

            if(leafLeft!=null && leafRight!=null){

            }else {
                log.info("");

            }



        }
    }


    public static void main(String[] args) {

        /*if(args.length != 3){
            throw new IllegalArgumentException("Parameter(s): <Self Port> <Discovery Node IP> <Discovery Node Port>" );
        }*/
        System.out.println("Parameter(s): <Self Port> <Discovery Node IP> <Discovery Node Port> <HEX ID optional>");

        discoverIP = args[1].trim();
        discoverPort = Integer.parseInt(args[2].trim());

        if(args.length==4){
            selfIdentifier = args[3].trim();
        }

        NodeMain nodeMain = new NodeMain(Integer.parseInt(args[0].trim()));
        nodeMain.startNode();

    }


    public boolean registerNode(){

        Socket discoverSocket = null;
        String requestType =null;
        String messRcvd = null;
        String[] messTokens = null;
        boolean regSuccess = false;


        while(!regSuccess) {

            try {
                discoverSocket = getSocket(discoverIP,discoverPort);
            } catch (IOException e) {
                log.error("Cannot connect to discovery Node.");
                return regSuccess;
                //e.printStackTrace();
            }


            if(selfIdentifier==null){
                selfIdentifier = getNodeIdentifier();
            }

            log.info("Identifier is " + selfIdentifier);

            String messToSend = "REG " + selfIP + " " + selfPort + " " + selfnickName + " " + selfIdentifier;

            log.info("Message to send to discovery Node : "+messToSend);

            try {
                sendDataToDestination(discoverSocket, messToSend);
                messRcvd = receiveDataFromDestination(discoverSocket);

            }catch (Exception e){
                log.error("Exception occured when trying to send message to discovery node");
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

            log.info("Message Received : " + messRcvd);

            messTokens = messRcvd.split(" ");
            requestType = messTokens[0].trim();
            System.out.println("Request type :"+requestType);

            if (requestType.equals("UNREGFIRST")) {
                log.info("Already the server is connected. First unregister and then register.");
                break;
            } else if (requestType.equals("IDCLASH")) {
                log.info("Already some other node has generated this id. Please use a different id.");
                break;
            }else if(requestType.equals("REGSUCCESS")){

                regSuccess = true;
                randomNodeID = Integer.parseInt(messTokens[1].trim());

                log.info("random id received : "+randomNodeID);

                if(randomNodeID!=0){
                    randomNodeIP = messTokens[2].trim().split(":")[0];
                    randomNodePort = Integer.parseInt(messTokens[2].trim().split(":")[1]);
                    randomNodeNickName = messTokens[3].trim();
                    randomNodeIdentifier = messTokens[4].trim();

                }

                log.info("Registration successful. come out of loop.");
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
