import org.apache.log4j.Logger;

import javax.xml.soap.Node;
import java.io.*;
import java.lang.reflect.Array;
import java.net.Inet4Address;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by leo on 9/27/15.
 */
public class NodeServer implements  Runnable{

    private Socket socket;
    private Socket nodeSocket;
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
            int hops = Integer.parseInt(tokens[1].trim());
            String newIPRec = tokens[2].trim();
            int newPortRec = Integer.parseInt(tokens[3].trim());
            String newNickNameRec = tokens[4].trim();
            String newIdentifierRec = tokens[5].trim();



            log.info("JOIN Message hop count "+hops+" Identifier : "+newIdentifierRec+" Ipaddress : "+newIPRec+" Port : "+newPortRec+" nick name "+newNickNameRec);

            // setting the leafset for the first guy.
            if(nodeMain.randomNodeID==0){
                log.info("I am the first guy and got a join request. so update the leaf set and routing table");

                // Setting the randomNodeID to 1 after processing the first guy.
                nodeMain.randomNodeID = 1;
                NodeDetails newNodeArrived = new NodeDetails(newIPRec,newPortRec,newIdentifierRec,newNickNameRec);

                nodeMain.leafLeft = newNodeArrived;
                nodeMain.leafRight = newNodeArrived;


                // Send the message to the node along with the leaf set as this is the first entry

                String messToSend = "FINALOK LEAF";


                try {
                    nodeSocket = getNodeSocket(newIPRec,newPortRec);
                    sendDataToDestination(nodeSocket,messToSend);
                } catch (IOException e) {
                    log.error("Exception occurred when trying to send JOINOK to Node.");
                    e.printStackTrace();
                }

                try {
                    sendObjectToDestination(nodeSocket,nodeMain.leafLeft);
                    sendObjectToDestination(nodeSocket, nodeMain.leafRight);
                    sendObjectToDestination(nodeSocket,selfNodeDetails);
                } catch (IOException e) {
                    log.error("Exception occured when sending object output.");
                    e.printStackTrace();
                }

                // Update the routing table
                int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
                char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);

                /*System.out.println("value in position " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getIdentifier());
                System.out.println("value in position " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getIpAddress());
                System.out.println("value in position " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getPort());
                System.out.println("value in position " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getNickName());*/

                nodeMain.routingTable.get(numberPrefixMatching).set(Integer.parseInt(firstNonMatchingPrefix+"",16),newNodeArrived);


                /*System.out.println("value in position after :  " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getIdentifier());
                System.out.println("value in position after :  " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getIpAddress());
                System.out.println("value in position after :  " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getPort());
                System.out.println("value in position after :  " + nodeMain.routingTable.get(numberPrefixMatching).get(Integer.parseInt(firstNonMatchingPrefix + "", 16)).getNickName());*/

            }else{
                // From third guy onwards.
                log.info("More than 2 Nodes are there in the system.  Now regular routing and updation");

                /* First send the first row to the guy.
                 Check if he falls between the leaf sets
                 if it doesnt fall within the leafset, then chk the routing*/

                // Sending the first row.
                // TBD

                // Chk if it falls between leaf sets

                int temprightLeaf = Integer.parseInt(nodeMain.leafRight.getIdentifier(),16);
                int templeftLeaf = Integer.parseInt(nodeMain.leafLeft.getIdentifier(), 16);
                int tempRecvdNodeIdenfifier = Integer.parseInt(newIdentifierRec,16);

                log.info("right "+temprightLeaf+" left "+templeftLeaf+" chk "+tempRecvdNodeIdenfifier );

                boolean betweenleafs = true;

                if(temprightLeaf!= templeftLeaf){
                    betweenleafs = isBetween(templeftLeaf,temprightLeaf,tempRecvdNodeIdenfifier);
                }


                System.out.println("is it between : "+betweenleafs);
                if(!betweenleafs){
                    // chk the routing table for closest match.
                    log.info("The value doesnt fall between two values.");
                    int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
                    char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
                    int firstNonMatchingPrefixInt = Integer.parseInt(firstNonMatchingPrefix+"",16);

                    log.info("Numer of prefix matching "+numberPrefixMatching+" firstnonmatching prefix "+firstNonMatchingPrefixInt);

                    log.info("Value in the arralist "+nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt).getIpAddress());

                    int index = firstNonMatchingPrefixInt;
                    String messToSend =null;
                    // Find the numerically closest guy from routing table and send it to that guy.
                    if(nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt).getIdentifier()!=null){
                        log.info("There is an entry in the non matching cell. Forward to that guy.");
                        messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec;
                    }else{

                        log.info("No entry in the non matching cell. So check for closest.");
                        index = getNumericallyCloserIndex(nodeMain.routingTable.get(numberPrefixMatching),newIdentifierRec);

                        if(nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier().equals(selfNodeDetails.getIdentifier())){
                            log.info("Closest one is me . I am responsile for him. So place him in the appropriate position. ");

                            // check to find if the node is greater or lesser and send the final message. Update the leafset also.
                            // TO DO



                        }else{
                            log.info("Numerically closes guy : "+nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier());
                            log.info("Have to send the packet to him. ");
                            messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec;

                        }

                        log.debug("Message to send "+ messToSend);

                    }

                    // Send join message

                    String newIpToSend = nodeMain.routingTable.get(numberPrefixMatching).get(index).getIpAddress();
                    int portTosend = nodeMain.routingTable.get(numberPrefixMatching).get(index).getPort();


                    try {
                        nodeSocket = getNodeSocket(newIpToSend,portTosend);
                        sendDataToDestination(nodeSocket,messToSend);
                    } catch (IOException e) {
                        log.error("Exception occured when trying to send message to destination");
                        e.printStackTrace();
                    }
                }else {
                    log.info("The value falls between the two leafsets. Place it at the proper place with wrapping.");



                }


            }




        }else if(requestType.equals("FINALOK")){

            String origLeafLeft = tokens[1].trim();

            if(origLeafLeft.equals("LEAF")){
                log.info("Getting LEAF entries from the last node in object output stream");

                NodeDetails nodeleft=null;
                NodeDetails nodeRight=null;
                NodeDetails nodeFinal=null;

                try {
                    nodeleft = (NodeDetails)receiveObjectFromDestination(socket);
                    nodeRight = (NodeDetails) receiveObjectFromDestination(socket);
                    nodeFinal = (NodeDetails) receiveObjectFromDestination(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    log.error("Exception occured when getting the object of leaf set");
                    e.printStackTrace();
                }


                // Second node. If both the selfNode Identifier and the received are equal. So update the leaf set and routing table.
                if(nodeleft.getIdentifier().equals(selfNodeDetails.getIdentifier()) && nodeRight.getIdentifier().equals(selfNodeDetails.getIdentifier())){
                    System.out.println("Second node. So update the leaf set to the first node. ");
                    nodeMain.leafLeft = new NodeDetails(nodeFinal.getIpAddress(),nodeFinal.getPort(),nodeFinal.getIdentifier(),nodeFinal.getNickName());
                    nodeMain.leafRight = new NodeDetails(nodeFinal.getIpAddress(),nodeFinal.getPort(),nodeFinal.getIdentifier(),nodeFinal.getNickName());

                    // Update the routing table
                    int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),nodeFinal.getIdentifier());
                    char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),nodeFinal.getIdentifier());

                    nodeMain.routingTable.get(numberPrefixMatching).set(Integer.parseInt(firstNonMatchingPrefix+"",16),nodeFinal);

                }else{
                    // TO DO Compute



                }

            }





        }else if(requestType.equals("NEWNODETABLE")){

        }


    }

    private int getNumericallyCloserIndex(ArrayList<NodeDetails> allNodesInRow,String newIdentifier){

        int newIdentifierInt = Integer.parseInt(newIdentifier,16);

        ArrayList<String> allIdentifiers = getAllIdentifiersInRow(allNodesInRow);


        int oldValue = 0;
        int newValue =0;
        int index =0;
        int tempvalue=0;

        for(tempvalue=0;tempvalue<allIdentifiers.size();tempvalue++){
            if(allIdentifiers.get(tempvalue)!=null){
                oldValue = Math.abs(Integer.parseInt(allIdentifiers.get(tempvalue),16)-newIdentifierInt);
                break;
            }

        }

        log.info("Temp value outside is "+tempvalue);

        for(int i=tempvalue;i<allIdentifiers.size();i++){

            if(allIdentifiers.get(i)!=null) {
                newValue = Math.abs(Integer.parseInt(allIdentifiers.get(i), 16) - newIdentifierInt);
                if (newValue <= oldValue) {
                    oldValue = newValue;
                    index = i;
                }
            }

        }
        log.debug("Numerically closer value to "+newIdentifier+" is : "+allIdentifiers.get(index));
        return index;

    }

    /*private ArrayList<Integer> getAllIdentifiersSortedToInt(ArrayList<String> allIdentifier){

        Iterator<String> itr = allIdentifier.iterator();
        ArrayList<Integer> allIdentifierInteger = new ArrayList<Integer>();

        while(itr.hasNext()){
            String identifierTemp = itr.next();
            allIdentifierInteger.add(Integer.parseInt(identifierTemp,16));

        }

        System.out.println("Before sorting");

        Iterator<Integer> itr1 = allIdentifierInteger.iterator();
        while(itr1.hasNext()){
            System.out.println("before storing "+itr1.next());
        }



        ArrayList<Integer> allIdentifierSorteInteger = new ArrayList<Integer>();
        Collections.sort(allIdentifierInteger);

        for(Integer id: allIdentifierInteger){
            allIdentifierSorteInteger.add(id);
        }


        Iterator<Integer> itr2 = allIdentifierSorteInteger.iterator();
        while(itr2.hasNext()){
            System.out.println("after storing "+itr2.next());
        }

        return allIdentifierInteger;


    }*/


    private ArrayList<String> getAllIdentifiersInRow(ArrayList<NodeDetails> allNodesInRow){

        Iterator<NodeDetails> itr = allNodesInRow.iterator();
        ArrayList<String> allIdentifier = new ArrayList<String>();

        while(itr.hasNext()){
            NodeDetails tempNode = itr.next();
            allIdentifier.add(tempNode.getIdentifier());

        }
        return  allIdentifier;

    }


    private char firstPreFixNotMatching(String firstIdentifier,String secondIdentifier){

        char[] fidentifier = firstIdentifier.toCharArray();
        char[] sidentifier = secondIdentifier.toCharArray();
        char prefixNotMatching='\0';

        int numberOfMatches =0;

        if(fidentifier.length==sidentifier.length) {
            for(int i=0;i<fidentifier.length;i++){
                if(fidentifier[i]!=sidentifier[i]){
                    prefixNotMatching = sidentifier[i];
                    break;
                }

            }
        }

        return prefixNotMatching;
    }

    private int numberOfPreFixMatching(String firstIdentifier,String secondIdentifier){

        char[] fidentifier = firstIdentifier.toCharArray();
        char[] sidentifier = secondIdentifier.toCharArray();

        int numberOfMatches =0;

        if(fidentifier.length==sidentifier.length) {
            for(int i=0;i<fidentifier.length;i++){
                if(fidentifier[i]==sidentifier[i]){
                    numberOfMatches++;
                }else{
                    break;
                }

            }
        }

        return numberOfMatches;
    }



    private boolean isBetween(int firstValue,int secondValue,int compareValue){

        if(firstValue!=secondValue) {

            if (firstValue < secondValue) {

                if (compareValue > firstValue && compareValue <= secondValue) {
                    return true;
                }
            } else {
                if (compareValue > firstValue || compareValue <= secondValue) {
                    return true;
                }
            }
        }
        return false;
    }

    private void sendObjectToDestination(Socket socket,Object object) throws IOException{

        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
        objectOutputStream.writeObject(object);
        objectOutputStream.flush();

    }

    private Object receiveObjectFromDestination(Socket socket) throws IOException, ClassNotFoundException {

        ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream());
        return  objectInputStream.readObject();

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

    private Socket getNodeSocket(String ipAddress, int port) throws IOException {
        return new Socket(ipAddress,port);
    }
}
