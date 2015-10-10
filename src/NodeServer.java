import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.*;

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



                //nodeMain.routingTable.get(numberPrefixMatching).set(Integer.parseInt(firstNonMatchingPrefix+"",16),newNodeArrived);



            }else{
                // From third guy onwards.
                log.info("More than 2 Nodes are there in the system.  Now regular routing and updation");

                /* First send the first row to the guy.
                 Check if he falls between the leaf sets
                 if it doesnt fall within the leafset, then chk the routing*/

                // Sending the first row.
                // TBD

                // Chk if it falls between leaf sets

                String messToSend =null;
                NodeDetails nodeDetailsToSend = lookup(newIdentifierRec);

                int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
                char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);

                System.out.println("Node details to send Identifier : "+nodeDetailsToSend.getIdentifier());
                System.out.println("Node details to send IPaddress : "+nodeDetailsToSend.getIpAddress());
                System.out.println("Node details to send port : "+nodeDetailsToSend.getPort());

                if(nodeDetailsToSend.getIdentifier().equals(selfNodeDetails.getIdentifier())){
                    System.out.println("I am the guy responsible for it. Find out where to place. Left or right. Same logic like leaf set.");



                }else{
                    messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec;
                }


                /*int temprightLeaf = Integer.parseInt(nodeMain.leafRight.getIdentifier(),16);
                int templeftLeaf = Integer.parseInt(nodeMain.leafLeft.getIdentifier(), 16);
                int tempRecvdNodeIdenfifier = Integer.parseInt(newIdentifierRec,16);

                log.info("Right "+nodeMain.leafRight.getIdentifier()+" left "+nodeMain.leafLeft.getIdentifier()+" rcvd "+newIdentifierRec);
                log.info("right "+temprightLeaf+" left "+templeftLeaf+" chk "+tempRecvdNodeIdenfifier );

                boolean betweenleafs = true;
                NodeDetails  nodedetailsTemp = null;

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

                    //int index = firstNonMatchingPrefixInt;
                    String messToSend =null;
                    // Find the numerically closest guy from routing table and send it to that guy.
                    if(nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt).getIdentifier()!=null){
                        log.info("There is an entry in the non matching cell. Forward to that guy.");
                        messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec;
                    }else{

                        log.info("No entry in the non matching cell. So check for closest.");
                        nodedetailsTemp = getNumericallyCloserIndex(newIdentifierRec);

                        if(nodedetailsTemp.getIdentifier().equals(selfNodeDetails.getIdentifier())){
                            log.info("Closest one is me . I am responsile for him. So place him in the appropriate position. ");

                            // check to find if the node is greater or lesser and send the final message. Update the leafset also.
                            // TO DO



                        }else{
                            log.info("Numerically closes guy : "+nodedetailsTemp.getIdentifier());
                            log.info("Have to send the packet to him. ");
                            messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec;

                        }

                        log.debug("Message to send "+ messToSend);

                    }

                    // Send join message

                    String newIpToSend = nodedetailsTemp.getIpAddress();
                    int portTosend = nodedetailsTemp.getPort();


                    try {
                        nodeSocket = getNodeSocket(newIpToSend,portTosend);
                        sendDataToDestination(nodeSocket,messToSend);
                    } catch (IOException e) {
                        log.error("Exception occured when trying to send message to destination");
                        e.printStackTrace();
                    }
                }else {
                    log.info("The value falls between the two leafsets. Place it at the proper place with wrapping.");
                    nodedetailsTemp = getNumericallyCloserIndex(newIdentifierRec);

                    System.out.println("Numberically closer : "+nodedetailsTemp.getIdentifier()+" ipaddress "+nodedetailsTemp.getIpAddress());
                    if(nodedetailsTemp.getIdentifier().equals(selfNodeDetails.getIdentifier())){
                        System.out.println("I am the closer node. Find ");
                    }

                }*/


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

                    //nodeMain.routingTable.get(numberPrefixMatching).set(Integer.parseInt(firstNonMatchingPrefix+"",16),nodeFinal);

                }else{
                    // TO DO Compute



                }

            }

        }else if(requestType.equals("NEWNODETABLE")){

        }


    }


    public NodeDetails lookup(String newIdentifierRec){

        int temprightLeaf = Integer.parseInt(nodeMain.leafRight.getIdentifier(),16);
        int templeftLeaf = Integer.parseInt(nodeMain.leafLeft.getIdentifier(), 16);
        int tempRecvdNodeIdenfifier = Integer.parseInt(newIdentifierRec,16);

        log.info("Right "+nodeMain.leafRight.getIdentifier()+" left "+nodeMain.leafLeft.getIdentifier()+" rcvd "+newIdentifierRec);
        log.info("right "+temprightLeaf+" left "+templeftLeaf+" chk "+tempRecvdNodeIdenfifier );

        boolean betweenleafs = true;
        NodeDetails  nodedetailsTemp = null;

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

            //int index = firstNonMatchingPrefixInt;
            String messToSend =null;
            // Find the numerically closest guy from routing table and send it to that guy.
            if(nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt).getIdentifier()!=null){
                log.info("There is an entry in the non matching cell. Forward to that guy.");
                //messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec;
                nodedetailsTemp = nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt);

            }else{

                log.info("No entry in the non matching cell. So check for closest.");
                nodedetailsTemp = getNumericallyCloserIndex(newIdentifierRec);

                /*if(nodedetailsTemp.getIdentifier().equals(selfNodeDetails.getIdentifier())){
                    log.info("Closest one is me . I am responsile for him. So place him in the appropriate position. ");

                    // check to find if the node is greater or lesser and send the final message. Update the leafset also.
                    // TO DO



                }else{
                    log.info("Numerically closes guy : "+nodedetailsTemp.getIdentifier());
                    log.info("Have to send the packet to him. ");
                    //messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec;
                    System.out.println("Send to that guy.");

                }*/

                //log.debug("Message to send "+ messToSend);

            }

            // Send join message

            /*String newIpToSend = nodedetailsTemp.getIpAddress();
            int portTosend = nodedetailsTemp.getPort();


            try {
                nodeSocket = getNodeSocket(newIpToSend,portTosend);
                sendDataToDestination(nodeSocket,messToSend);
            } catch (IOException e) {
                log.error("Exception occured when trying to send message to destination");
                e.printStackTrace();
            }*/
        }else {
            log.info("The value falls between the two leafsets. Place it at the proper place with wrapping.");
            nodedetailsTemp = getNumericallyCloserIndex(newIdentifierRec);

            System.out.println("Numberically closer : "+nodedetailsTemp.getIdentifier()+" ipaddress "+nodedetailsTemp.getIpAddress());

        }
        return nodedetailsTemp;
    }




    private NodeDetails getNumericallyCloserIndex(String newIdentifier){


        ArrayList<Integer> allIdentifiers = new ArrayList<Integer>();
        ArrayList<String> allIdentifiersString = new ArrayList<String>();

        for(int i=0;i<4;i++){

            ArrayList<NodeDetails> allNodeDetailsTemp = nodeMain.routingTable.get(i);

            Iterator<NodeDetails> itr = allNodeDetailsTemp.iterator();

            while (itr.hasNext()){
                NodeDetails nodeDetails = itr.next();

                if(nodeDetails.getIdentifier()!=null){
                    //allIdentifiers.add(Integer.parseInt(nodeDetails.getIdentifier(),16));
                    allIdentifiersString.add(nodeDetails.getIdentifier());
                }
            }
        }


        // Removing duplicates as there might be the same value in different rows
        Set setItems = new LinkedHashSet<String>(allIdentifiersString);
        allIdentifiersString.clear();
        allIdentifiersString.addAll(setItems);

        // Sorting all the identifiers and checking.
        Collections.sort(allIdentifiersString);

        // Convert the hex to interger
        Iterator<String> itr = allIdentifiersString.iterator();

        while (itr.hasNext()){
            allIdentifiers.add(Integer.parseInt(itr.next(),16));

        }


        int selfNodeIdentifierInt = Integer.parseInt(selfNodeDetails.getIdentifier(), 16);

        int arrLastValue = allIdentifiers.get(allIdentifiers.size() - 1);
        int arrFirstValue = allIdentifiers.get(0);
        System.out.println(arrFirstValue);

        int finalValue = Integer.parseInt("FFFF", 16);
        int newIdentifierInt = Integer.parseInt(newIdentifier, 16);

        int numbericallyCloserIndex = 0;

        if (newIdentifierInt < allIdentifiers.get(0)) {
            //System.out.println("The value is before the first value in the list.");

            if ((arrFirstValue - newIdentifierInt) <= ((finalValue - arrLastValue) + newIdentifierInt)) {
                System.out.println("Value closer or equal is the first value.----" + allIdentifiers.get(0));
            } else {
                System.out.println("Last value in array closer****" + allIdentifiers.get(allIdentifiers.size() - 1));
                numbericallyCloserIndex = allIdentifiers.size() - 1;
            }

        } else if (newIdentifierInt > allIdentifiers.get(allIdentifiers.size() - 1)) {
            //System.out.println("The value is greater than the last value in the list. ");
            System.out.println("compare - arrlastvalue " + (newIdentifierInt - arrLastValue) + " ((finishvalue-compare)+arrfirstvalue ) " + ((finalValue - newIdentifierInt) + arrFirstValue));

            if ((newIdentifierInt - arrLastValue) < ((finalValue - newIdentifierInt) + arrFirstValue)) {
                System.out.println("value closer to the last value in array .... " + allIdentifiers.get(allIdentifiers.size() - 1));
                numbericallyCloserIndex = allIdentifiers.size() - 1;
            } else {
                System.out.println("Equal or closer is the first value in the array ::: " + allIdentifiers.get(0));

            }
        } else {
            System.out.println("The value falls between two nodes that are already present.");

            System.out.println(allIdentifiers.get(0)+" "+allIdentifiers.get(1)+" "+newIdentifierInt);
            int oldvalue = Math.abs(allIdentifiers.get(0) - newIdentifierInt);
            int index = 0;
            int newvale = 0;
            for (int i = 1; i < allIdentifiers.size(); i++) {
                newvale = Math.abs(allIdentifiers.get(i) - newIdentifierInt);

                //System.out.println("New value "+newvale+" smaller "+arrInt.get(i)+" old value "+oldvalue);
                if (newvale <= oldvalue) {
                    oldvalue = newvale;
                    index = i;

                }

            }
            numbericallyCloserIndex = index;
            System.out.println("New value "+newvale);
            System.out.println("oldvalue " + oldvalue + " index " + index + " value " + allIdentifiers.get(numbericallyCloserIndex));


        }

        String closerIdentifier = allIdentifiersString.get(numbericallyCloserIndex);

        // Return the the NodeDetails object for reference.

        boolean nodeDetails = false;
        NodeDetails tempNode =null;

        for(int i=0;i<4;i++) {

            ArrayList<NodeDetails> allNodeDetailsTemp = nodeMain.routingTable.get(i);

            Iterator<NodeDetails> itr3 = allNodeDetailsTemp.iterator();

            while (itr3.hasNext()) {
                tempNode = itr3.next();
                if(tempNode.getIdentifier()!=null) {
                    if (tempNode.getIdentifier().equals(closerIdentifier)) {

                        nodeDetails = true;
                        break;
                    }
                }
            }
            if(nodeDetails){
                break;
            }
        }

        return tempNode;


    }

    private ArrayList<String> getAllIdentifiersInRow(ArrayList<NodeDetails> allNodesInRow){

        Iterator<NodeDetails> itr = allNodesInRow.iterator();
        ArrayList<String> allIdentifier = new ArrayList<String>();

        while(itr.hasNext()){
            NodeDetails tempNode = itr.next();
            allIdentifier.add(tempNode.getIdentifier());

        }
        return  allIdentifier;

    }

    private ArrayList<Integer> getAllIdentifiersIntegers(ArrayList<NodeDetails> allNodesInRow){

        ArrayList<String> allIdentifierString = getAllIdentifiersInRow(allNodesInRow);

        ArrayList<Integer> allIdentifierInteger = new ArrayList<Integer>();

        Iterator<String> itr = allIdentifierString.iterator();
        while (itr.hasNext()){
            allIdentifierInteger.add(Integer.parseInt(itr.next(),16));


        }
        return allIdentifierInteger;


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
