import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
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
            String pathTravelled = tokens[6].trim();

            Map<Integer,ArrayList<NodeDetails>> tempRoutingTable=null;

            try {
                tempRoutingTable = (Map<Integer,ArrayList<NodeDetails>>)receiveObjectFromDestination(socket);
            } catch (ClassNotFoundException e1) {
                System.out.println("Exception occured when getting the tempRouting table");
                e1.printStackTrace();
            } catch (IOException e1) {
                System.out.println("Exception occured when trying to get the temprouting table");
                e1.printStackTrace();
            }

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

                int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
                char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);


                // update routing table 
                System.out.println("number of prefix matching "+numberPrefixMatching);
                Map<Integer,ArrayList<NodeDetails>> receivedTempRoutingTable = updateRoutingTable(tempRoutingTable, numberPrefixMatching);

                // Send to the originator

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
                    sendObjectToDestination(nodeSocket, receivedTempRoutingTable);

                } catch (IOException e) {
                    log.error("Exception occured when sending object output.");
                    e.printStackTrace();
                }






            }else{
                // From third guy onwards.
                log.info("More than 2 Nodes are there in the system.  Now regular routing and updation");

                /* First send the first row to the guy.
                 Check if he falls between the leaf sets
                 if it doesnt fall within the leafset, then chk the routing*/




                String messToSend =null;
                NodeDetails nodeDetailsToSend = lookup(newIdentifierRec);

                int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
                char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);


                // update the routing table.

                Map<Integer,ArrayList<NodeDetails>> receivedTempRoutingTable = updateRoutingTable(tempRoutingTable, numberPrefixMatching);

                if(nodeDetailsToSend.getIdentifier().equals(selfNodeDetails.getIdentifier())){
                    log.info("I am the guy responsible for the JOIN .YOU Find out where to place. Left or right.");
                    log.info("Send JOIN OK");
                    messToSend = "FINALOK LEAF";

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
                        sendObjectToDestination(nodeSocket, receivedTempRoutingTable);
                    } catch (IOException e) {
                        log.error("Exception occured when sending object output.");
                        e.printStackTrace();
                    }



                    System.out.println("Path Travelled : "+pathTravelled);


                }else{

                    pathTravelled = pathTravelled + selfNodeDetails.getIdentifier()+"=>";
                    messToSend = "JOIN "+ ++hops +" "+ newIPRec +" "+newPortRec+" "+newNickNameRec+" "+newIdentifierRec+" "+" "+pathTravelled;

                    try {
                        nodeSocket = getNodeSocket(nodeDetailsToSend.getIpAddress(),nodeDetailsToSend.getPort());
                        sendDataToDestination(nodeSocket,messToSend);
                        sendObjectToDestination(nodeSocket, receivedTempRoutingTable);
                    } catch (IOException e) {
                        log.error("Exception occurred when trying to send JOIN to Node.");
                        e.printStackTrace();
                    }


                }



            }


        }else if(requestType.equals("FINALOK")){

            String origLeafLeft = tokens[1].trim();
            String messToSend=null;

            if(origLeafLeft.equals("LEAF")){
                log.info("Getting LEAF entries from the last node in object output stream");

                NodeDetails nodeleft=null;
                NodeDetails nodeRight=null;
                NodeDetails nodeFinal=null;
                Map<Integer,ArrayList<NodeDetails>> routingTableTemp=null;


                try {
                    nodeleft = (NodeDetails)receiveObjectFromDestination(socket);
                    nodeRight = (NodeDetails) receiveObjectFromDestination(socket);
                    nodeFinal = (NodeDetails) receiveObjectFromDestination(socket);
                    routingTableTemp = (Map<Integer,ArrayList<NodeDetails>>)receiveObjectFromDestination(socket);
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    log.error("Exception occured when getting the object of leaf set");
                    e.printStackTrace();
                }

                // Update the routing table
                for(int j=0;j<4;j++){

                    for(int i=0;i<routingTableTemp.get(j).size();i++){

                        if(nodeMain.routingTable.get(j).get(i).getIdentifier()==null){
                            //System.out.println("Value is null ");

                            if(routingTableTemp.get(j).get(i).getIdentifier()!=null){
                                //System.out.println("Here it is present... grab it ");
                                nodeMain.routingTable.get(j).set(i,routingTableTemp.get(j).get(i));
                            }

                        }
                    }
                }



                // Second node. If both the selfNode Identifier and the received are equal. So update the leaf set.
                if(nodeleft.getIdentifier().equals(selfNodeDetails.getIdentifier()) && nodeRight.getIdentifier().equals(selfNodeDetails.getIdentifier())){

                    log.info("Second node. So update the leaf set to the first node. ");
                    nodeMain.leafLeft = new NodeDetails(nodeFinal.getIpAddress(),nodeFinal.getPort(),nodeFinal.getIdentifier(),nodeFinal.getNickName());
                    nodeMain.leafRight = new NodeDetails(nodeFinal.getIpAddress(),nodeFinal.getPort(),nodeFinal.getIdentifier(),nodeFinal.getNickName());


                }else{

                    boolean clockwise=true;
                    NodeDetails destinationSendLeaf;

                    int selfNodeInt = Integer.parseInt(selfNodeDetails.getIdentifier(),16);
                    int finalNodeInt = Integer.parseInt(nodeFinal.getIdentifier(),16);

                    int distance = finalNodeInt - selfNodeInt;
                    int reverse = Integer.parseInt("FFFF",16) - distance;
                    int mindistance = Math.min(Math.abs(distance),Math.abs(reverse));

                    if(mindistance==Math.abs(distance)){
                        if(selfNodeInt < finalNodeInt){
                            clockwise = true;
                        }else{
                            clockwise =false;
                        }
                    }else{
                        if(selfNodeInt< finalNodeInt){
                            clockwise = false;
                        }else{
                            clockwise = true;
                        }
                    }

                    if(clockwise){
                        //destinationSendLeaf = nodeleft;

                        log.info("Clockwise");
                        nodeMain.leafLeft= nodeFinal;
                        nodeMain.leafRight = nodeRight;

                        messToSend = "CONVERGELEAF 1";

                        try {
                            nodeSocket = getNodeSocket(nodeFinal.getIpAddress(),nodeFinal.getPort());
                            sendDataToDestination(nodeSocket,messToSend);
                            sendObjectToDestination(nodeSocket, selfNodeDetails);
                        } catch (IOException e) {
                            log.info("Exception occured when trying to send the routing table details.");

                            e.printStackTrace();
                        }

                        messToSend = "CONVERGELEAF 0";

                        try {
                            nodeSocket = getNodeSocket(nodeRight.getIpAddress(),nodeRight.getPort());
                            sendDataToDestination(nodeSocket,messToSend);
                            sendObjectToDestination(nodeSocket, selfNodeDetails);
                        } catch (IOException e) {
                            log.info("Exception occured when trying to send the routing table details.");

                            e.printStackTrace();
                        }


                    }else{
                        //destinationSendLeaf = nodeRight;
                        log.info("Anticlockwise.");
                        nodeMain.leafLeft=nodeleft;
                        nodeMain.leafRight = nodeFinal;

                        messToSend = "CONVERGELEAF 0";

                        try {
                            nodeSocket = getNodeSocket(nodeFinal.getIpAddress(),nodeFinal.getPort());
                            sendDataToDestination(nodeSocket,messToSend);
                            sendObjectToDestination(nodeSocket, selfNodeDetails);
                        } catch (IOException e) {
                            log.info("Exception occured when trying to send the routing table details.");

                            e.printStackTrace();
                        }

                        messToSend= "CONVERGELEAF 1";

                        try {
                            nodeSocket = getNodeSocket(nodeleft.getIpAddress(),nodeleft.getPort());
                            sendDataToDestination(nodeSocket,messToSend);
                            sendObjectToDestination(nodeSocket, selfNodeDetails);
                        } catch (IOException e) {
                            log.info("Exception occured when trying to send the routing table details.");

                            e.printStackTrace();
                        }


                    }


                }
                sendRoutingTableUpdateToNodes();

            }

        }else if(requestType.equals("CONVERGELEAF")){


            int side = Integer.parseInt(tokens[1].trim());

            NodeDetails nodeDetails=null;
            try {
                nodeDetails = (NodeDetails) receiveObjectFromDestination(socket);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            if(side== 1){
                log.info("Change your right node");
                nodeMain.leafRight = nodeDetails;
            }else if(side==0){
                log.info("Change your left node");
                nodeMain.leafLeft = nodeDetails;
            }

            // See the files that are applicable for the node that joined and send him the files.
            // Remove from the files datastructre.

            // Delete the files from the storage /s/

            // Find the files in the system and disperse it.

            Set<String> fileKeys = nodeMain.fileStoredDetails.keySet();


            if(fileKeys.size()==0){
                System.out.println("No files stored in the system.");
            }else{

                Iterator<String> itr2 = fileKeys.iterator();
                while (itr2.hasNext()){
                    String hashIdentifier = itr2.next();

                    String nodeIdentifier = nodeDetails.getIdentifier();
                    String selfIdentifier = selfNodeDetails.getIdentifier();

                    String closestValue = closestMatch(hashIdentifier,nodeIdentifier,selfIdentifier);

                    // prepare to send the data.
                    File file = new File(StoreDataStart.fileName);
                    FileInputStream fileInputStream = null;
                    try {
                        fileInputStream = new FileInputStream(file);
                    }catch (IOException e){
                        log.error("Exception occured when trying to get the stream.");
                    }

                    int fileSize = (int)file.length();
                    byte[] fileByte = new byte[(int) fileSize];

                    try {
                        int numberRead = fileInputStream.read(fileByte, 0, fileSize);
                    } catch (IOException e) {
                        log.error("Exception occured when trying to read bytes.");
                        e.printStackTrace();
                    }

                    String messToSend = "FILEDATA "+nodeMain.fileStoredDetails.get(hashIdentifier)+" "+hashIdentifier+" "+fileSize;


                    if(closestValue.equals(nodeIdentifier)){

                        log.info("Send the file to the New node.");
                        try {
                            Socket nodeSocket = getNodeSocket(nodeDetails.getIpAddress(), nodeDetails.getPort());
                            sendDataToDestination(nodeSocket, messToSend);

                            // Send the files to the node.

                            DataOutputStream dataOutputStream = new DataOutputStream(nodeSocket.getOutputStream());
                            dataOutputStream.writeInt(fileSize);
                            dataOutputStream.write(fileByte,0,fileSize);
                            dataOutputStream.flush();

                        } catch (IOException e) {
                            log.error("Exception occured when trying to send removal message.");
                            e.printStackTrace();
                        }

                        log.info(" File datastrucutre  before "+nodeMain.fileStoredDetails.size());
                        nodeMain.fileStoredDetails.remove(hashIdentifier);
                        log.info(" File datastrucutre after "+nodeMain.fileStoredDetails.size());

                    }else {
                        log.info("Keep the file locally. I am the closer.");
                    }


                }




            }





        }else if(requestType.equals("REMNODELEAFUPDATE")){
        	
        	int leftOrRight= Integer.parseInt(tokens[1].trim());

            NodeDetails nodeDetails=null;
            NodeDetails nodeRemoved = null;

            try {
                nodeDetails = (NodeDetails) receiveObjectFromDestination(socket);

            }catch (IOException e){
                log.error("Exception occured when getting the object form source.");
            } catch (ClassNotFoundException e) {
                log.error("Exception occured when getting the object of leaf set");
                e.printStackTrace();
            }

        	
        	if(leftOrRight==1){
        		log.info("update my right leaf with the receivd right.");

        		// update my Right to the received Right
                nodeRemoved = nodeMain.leafRight;
        		nodeMain.leafRight = nodeDetails;

        		
        	}else if(leftOrRight==0){
        		log.info("update my left the received left.");
        		
        		// Update my left to Received left
                nodeRemoved = nodeMain.leafRight;
        		nodeMain.leafLeft = nodeDetails;

        	}

        }

        /*else if(requestType.equals("NEWNODETABLE")) {

            int numberOfRows = Integer.parseInt(tokens[1].trim());
            String rowNumbersTemp = tokens[2].trim();
            String[] rows = rowNumbersTemp.split(",");

            for (int i = 0; i <numberOfRows; i++) {
                try {
                    ArrayList<NodeDetails> allNodesDetailRow = (ArrayList<NodeDetails>) receiveObjectFromDestination(socket);
                    updateRoutingTable(allNodesDetailRow,rows[i]);
                    log.info("Routing Table updated. Send a message all the Nodes in the routing table about update.");

                    sendRoutingTableUpdateToNodes();

                } catch (IOException e) {
                    log.error("Excepection occured when trying to retrievie data from destnation");

                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }*/else if(requestType.equals("ROUTINGTABLECONV")){

            String rcvdIdentifier = tokens[1].trim();
            int rowNumber = Integer.parseInt(tokens[2].trim());

            try {

                ArrayList<NodeDetails> routingTable = (ArrayList<NodeDetails>)receiveObjectFromDestination(socket);

                log.info("Routing table update received from "+rcvdIdentifier);
                updateFinalRoutingTable(routingTable,rowNumber);

            } catch (IOException e) {
                log.info("Exceptinon occured when trying to get routing table.");
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }


        }else if(requestType.equals("GETNODE")){

            String typeOfOperation = tokens[1].trim();
            String identifierFile = tokens[2].trim();
            String storeIPToSend = tokens[3].trim();
            int storePortToSend = Integer.parseInt(tokens[4].trim());
            String pathTravelled = tokens[5].trim();

            //System.out.println("GETNODE received............");

            // Check the closest node and append the path travelled.
            NodeDetails fileNodeDetails = lookup(identifierFile);
            String messToSend = "";

            log.info("Message received from "+storeIPToSend+" Port "+storePortToSend);

            log.info("Message received to send the closest node for saving the file. Path Travelled "+pathTravelled);
            System.out.println("self node details "+selfNodeDetails.getIdentifier());

            if(typeOfOperation.equals("FILESAVE")){

                if(fileNodeDetails.getIdentifier().equals(selfNodeDetails.getIdentifier())){
                    log.info("I am the guy responsible for the file. Send a message to the destination to send the file.");
                    pathTravelled = pathTravelled+"=>"+selfNodeDetails.getIdentifier()+"=>FINAL";

                    log.info("Path Travelled so far : "+pathTravelled);

                    messToSend = "CLOSESTSERVER "+typeOfOperation+" "+fileNodeDetails.getIpAddress()+":"+fileNodeDetails.getPort()
                            +" "+fileNodeDetails.getIdentifier()+" "+pathTravelled;

                    try {
                        nodeSocket = getNodeSocket(storeIPToSend, storePortToSend);
                    } catch (IOException e) {
                        System.out.println("Exception occued when trying to get datastore socket");
                        e.printStackTrace();
                    }

                }else{
                    log.info("Forward the message to the closest node. ");
                    System.out.println("self node details --"+selfNodeDetails.getIdentifier());
                    pathTravelled = pathTravelled+" => "+selfNodeDetails.getIdentifier();
                    System.out.println("Path travelled ==="+pathTravelled);

                    messToSend = "GETNODE "+typeOfOperation+" "+identifierFile+" "+storeIPToSend+" "+storePortToSend+" "+pathTravelled;
                    try {
                        nodeSocket = getNodeSocket(fileNodeDetails.getIpAddress(), fileNodeDetails.getPort());
                    } catch (IOException e) {
                        System.out.println("Exception occued when trying to get closest node socket");
                        e.printStackTrace();
                    }

                }
            }else if(typeOfOperation.equals("FILERET")){

                // TO DO Retreive operation.


            }
            try {

                sendDataToDestination(nodeSocket,messToSend);

            } catch (IOException e) {
                System.out.println("Exception occured when trying to send the message to Node.");
                e.printStackTrace();
            }




        }else if(requestType.equals("FILEDATA")){

            //receive the file from destination.
            String fileName = tokens[1].trim();
            String fileNameHash = tokens[2].trim();
            //int fileSize = Integer.parseInt(tokens[3].trim());

            DataInputStream dataInputStream = null;

            try{
                dataInputStream = new DataInputStream(socket.getInputStream());
                int messageLength = dataInputStream.readInt();
                System.out.println(messageLength);
                byte[] data = new byte[messageLength];
                dataInputStream.readFully(data, 0, messageLength);
                System.out.println("File name received "+fileName);
                File newFileName = new File("/tmp/hsperfdata_leovig"+fileName);

                if(!newFileName.exists()){
                    newFileName.getParentFile().mkdirs();
                }

                FileOutputStream fileOutputStream = new FileOutputStream(newFileName);
                fileOutputStream.write(data);
                fileOutputStream.close();
                // Update data structure.
                nodeMain.fileStoredDetails.put(fileNameHash, fileName);

            }catch(IOException e){
                System.out.println("Exception occured when trying to get the file.");
                e.printStackTrace();
            }

        }


    }

    private byte[] convertFileToByteArray(String fileName) throws IOException{

        return Files.readAllBytes(new File(fileName).toPath());

    }

    private void updateFinalRoutingTable(ArrayList<NodeDetails> routingTablePart,int rowNumber){

        for(int j=0;j<16;j++){

            if(nodeMain.routingTable.get(rowNumber).get(j).getIdentifier()==null){

                if(routingTablePart.get(j).getIdentifier()!=null){

                    nodeMain.routingTable.get(rowNumber).set(j,routingTablePart.get(j));

                }
            }

        }

    }



    private void sendRoutingTableUpdateToNodes(){


        ArrayList<String> alreadySent = new ArrayList<String>();

        for(int i=0;i<4;i++){

            Iterator<NodeDetails> itr = nodeMain.routingTable.get(i).iterator();

            while (itr.hasNext()){

                NodeDetails nodeDetailsTemp = itr.next();

                if(nodeDetailsTemp.getIdentifier()!=null){

                    String ipAddressTemp  = nodeDetailsTemp.getIpAddress();
                    int portTemp = nodeDetailsTemp.getPort();

                    if(!nodeDetailsTemp.getIdentifier().equals(selfNodeDetails.getIdentifier()) && !alreadySent.contains(nodeDetailsTemp.getIdentifier())) {

                        //System.out.println("Send to ip : "+ipAddressTemp);
                        String messToSend = "ROUTINGTABLECONV "+nodeDetailsTemp.getIdentifier()+" "+i;
                        try {

                            System.out.println("preparing to send "+nodeDetailsTemp.getIdentifier());
                            nodeSocket = getNodeSocket(ipAddressTemp,portTemp);
                            sendDataToDestination(nodeSocket,messToSend);
                            sendObjectToDestination(nodeSocket,nodeMain.routingTable.get(i));
                            System.out.println("Routing table information sent to "+nodeDetailsTemp.getIdentifier());

                            alreadySent.add(nodeDetailsTemp.getIdentifier());
                        } catch (IOException e) {
                            log.error("Exception occured when trying to get node socket.");
                            e.printStackTrace();
                        }

                    }

                }


            }


        }



    }



    private Map<Integer,ArrayList<NodeDetails>>  updateRoutingTable(Map<Integer,ArrayList<NodeDetails>> routingTableTemp,int rowsMatching){

        System.out.println("Updating routing table.");

        System.out.println("Row number of current routing table : "+rowsMatching);

        for(int j=0;j<=rowsMatching;j++){

            for(int i=0;i<routingTableTemp.get(j).size();i++){

                if(routingTableTemp.get(j).get(i).getIdentifier()==null){
                    //System.out.println("Value is null ");

                    if(nodeMain.routingTable.get(j).get(i).getIdentifier()!=null){
                        //System.out.println("Here it is present... grab it ");
                        routingTableTemp.get(j).set(i,nodeMain.routingTable.get(j).get(i));
                    }

                }else{
                    System.out.println("Value is present...");
                }
            }
        }
        return routingTableTemp;


    }


    public NodeDetails lookup(String newIdentifierRec){

        String right = nodeMain.leafRight.getIdentifier();
        String left =nodeMain.leafLeft.getIdentifier();

        int temprightLeaf = Integer.parseInt(right,16);
        int templeftLeaf = Integer.parseInt(left, 16);
        int tempRecvdNodeIdenfifier = Integer.parseInt(newIdentifierRec,16);

        log.info("Right "+nodeMain.leafRight.getIdentifier()+" left "+nodeMain.leafLeft.getIdentifier()+" rcvd "+newIdentifierRec);
        //log.info("right "+temprightLeaf+" left "+templeftLeaf+" chk "+tempRecvdNodeIdenfifier );

        boolean betweenleafs = true;
        NodeDetails  nodedetailsTemp = null;

        if(temprightLeaf!= templeftLeaf){
            betweenleafs = isBetween(temprightLeaf,templeftLeaf,tempRecvdNodeIdenfifier);
        }


        System.out.println("is it between : "+betweenleafs);

        if(!betweenleafs){
            // chk the routing table for closest match.
            log.info("The value doesnt fall between two values.");
            int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
            char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
            int firstNonMatchingPrefixInt = Integer.parseInt(firstNonMatchingPrefix+"",16);

            log.info("Numer of prefix matching "+numberPrefixMatching+" firstnonmatching prefix "+firstNonMatchingPrefixInt);

            int index = firstNonMatchingPrefixInt;
            String messToSend =null;
            // Find the numerically closest guy from routing table and send it to that guy.
            if(nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt).getIdentifier()!=null){
                log.info("NOTBETWEENLEWFS : There is an entry in the non matching cell. Forward to that guy.");
                nodedetailsTemp = nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt);
                log.info("NOTBETWEENLEWFS : Messge to be sent to  "+nodedetailsTemp.getIdentifier());
            }else{

                log.info("No entry in the non matching cell. So check for closest.");
                index = getNumericallyCloserIndex(numberPrefixMatching,newIdentifierRec);
                //System.out.println("Closest index received "+index+" routing table "+nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier()+" self node "+selfNodeDetails.getIdentifier());

                if(nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier().equals(selfNodeDetails.getIdentifier())){

                    log.info("Check if I am the closer or the leaf sets are closer.");
                    // Check the leaf set to see who is closer. 

                    String closestMatchLeafSet = closestMatch(newIdentifierRec,left,right);
                    String closestMatch = closestMatch(newIdentifierRec,closestMatchLeafSet,selfNodeDetails.getIdentifier());

                    if(closestMatch.equals(selfNodeDetails.getIdentifier())){
                        log.info("I am the closest match. I need to place it.");
                        nodedetailsTemp = selfNodeDetails;
                    }else if(closestMatch.equals(left)){
                        log.info("Left is closer. send it");
                        nodedetailsTemp = nodeMain.leafLeft;
                    }else if(closestMatch.equals(right)){
                        log.info("Right is closer send it");
                        nodedetailsTemp = nodeMain.leafRight;
                    }


                }else{
                    log.info("Numerically closes guy : " + nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier());
                    log.info("Have to send the packet to him. ");

                    nodedetailsTemp = nodeMain.routingTable.get(numberPrefixMatching).get(index);

                }
            }


        }else {
            log.info("The value falls between the two leafsets. Place it at the proper place with wrapping.");

            String closestMatchLeafSet = closestMatch(newIdentifierRec,left,right);
            String closestMatch = closestMatch(newIdentifierRec,closestMatchLeafSet,selfNodeDetails.getIdentifier());

            if(closestMatch.equals(selfNodeDetails.getIdentifier())){
                log.info("I am the closest match. I need to place it.");
                nodedetailsTemp = selfNodeDetails;
            }else if(closestMatch.equals(left)){
                log.info("Left is closer. send it");
                nodedetailsTemp = nodeMain.leafLeft;
            }else if(closestMatch.equals(right)){
                log.info("Right is closer send it");
                nodedetailsTemp = nodeMain.leafRight;
            }
        }

        log.info("Final Closest Match for : "+newIdentifierRec+" is : "+nodedetailsTemp.getIdentifier());

        return nodedetailsTemp;
    }

    private String closestMatch(String compareValue,String left,String right) {

        int leftNum = Integer.parseInt(left, 16);
        int rightNum = Integer.parseInt(right, 16);
        int compareValueNum = Integer.parseInt(compareValue, 16);

        System.out.println(rightNum + " " + leftNum + " " + compareValueNum);

        int nodeLeft = leftNum - compareValueNum;
        int reversrLeft = nodeLeft - Integer.parseInt("FFFF", 16);
        int first = Math.min(Math.abs(nodeLeft), Math.abs(reversrLeft));


        int nodeRigh = rightNum - compareValueNum;
        int reversrRight = nodeRigh - Integer.parseInt("FFFF", 16);
        int second = Math.min(Math.abs(nodeRigh), Math.abs(reversrRight));

        log.info("	 " + first + " Min second " + second);

        if (first < second) {
            return left;

        } else if (first > second) {
            return right;
        } else {

            if (leftNum < nodeRigh) {
                return right;
            }
            return left;
        }
    }



    private int getNumericallyCloserIndex(int numberPrefixMatching,String compareValue){

        ArrayList<NodeDetails> nodeDetailsArrayList = nodeMain.routingTable.get(numberPrefixMatching);
        ArrayList<String> allIdentifierString = getAllIdentifiersInRow(nodeDetailsArrayList);

        System.out.println("all identifier string size "+allIdentifierString.size());

        Iterator<NodeDetails> itr = nodeDetailsArrayList.iterator();
        int closestIndex=0;
        String closetValue = null;

        while (itr.hasNext()){
            closetValue = itr.next().getIdentifier();
            if(closetValue!=null){
                break;
            }
            closestIndex++;
        }

        /*System.out.println("size of all identifier "+allIdentifierString.size());
        System.out.println("First item value "+closetValue);*/

        if(allIdentifierString.size()>1) {

            //System.out.println("More than one item in the row found.");
            for(int i=0;i<nodeDetailsArrayList.size();i++){


                if(nodeDetailsArrayList.get(i).getIdentifier()!=null){

                    //System.out.println("value in the identifier "+nodeDetailsArrayList.get(i).getIdentifier()+" closestvalue :: "+closetValue+" i value "+i);


                    if(!nodeDetailsArrayList.get(i).getIdentifier().equals(closetValue)){
                        String secondValue = nodeDetailsArrayList.get(i).getIdentifier();
                        closetValue = closestMatch(compareValue,closetValue,secondValue);
                        if(closetValue.equals(secondValue)){
                            closestIndex = i;
                        }
                        //System.out.println("Second value "+secondValue+" closer value "+closetValue+" closet index "+i);
                    }else{
                        //System.out.println("FirstValue found. So leaving it. "+ nodeDetailsArrayList.get(i).getIdentifier());
                    }

                }

            }

        }
        //log.info("Closest index  :  "+closestIndex);

        return closestIndex;
    }



    /*private int getNumericallyCloserIndex(int numberPrefixMatching,String newIdentifier){


        ArrayList<NodeDetails> nodeDetailsArrayList = nodeMain.routingTable.get(numberPrefixMatching);

        ArrayList<Integer> allIdentifiers = getAllIdentifiersIntegers(nodeDetailsArrayList);


        int selfNodeIdentifierInt = Integer.parseInt(selfNodeDetails.getIdentifier(), 16);

        int arrLastValue = allIdentifiers.get(allIdentifiers.size() - 1);
        int arrFirstValue = allIdentifiers.get(0);
        //System.out.println("size "+allIdentifiers.size());

        int finalValue = Integer.parseInt("FFFF", 16);
        int newIdentifierInt = Integer.parseInt(newIdentifier, 16);

        int numbericallyCloserIndex = 0;

        if (newIdentifierInt < allIdentifiers.get(0)) {
            //System.out.println("The value is before the first value in the list.");

            if ((arrFirstValue - newIdentifierInt) <= ((finalValue - arrLastValue) + newIdentifierInt)) {
                //System.out.println("Value closer or equal is the first value.----" + allIdentifiers.get(0));
                log.info("First value is the closer or equal "+allIdentifiers.get(0));
                int closestMath = allIdentifiers.get(0);
                String closestMathHex = Integer.toHexString(closestMath);

                Iterator<NodeDetails> itr = nodeMain.routingTable.get(numberPrefixMatching).iterator();
                int i=0;
                while(itr.hasNext()){
                    NodeDetails nodeDetailsTemp = itr.next();

                    if(nodeDetailsTemp.getIdentifier()!=null) {
                        if (nodeDetailsTemp.getIdentifier().equals(closestMathHex)) {
                            numbericallyCloserIndex = i;
                            break;
                        }
                    }
                    i++;
                }

            } else {
                log.info("Last value is closer :" + allIdentifiers.get(allIdentifiers.size() - 1));
                int closestMath = allIdentifiers.get(allIdentifiers.size() - 1);
                String closestMathHex = Integer.toHexString(closestMath);

                Iterator<NodeDetails> itr = nodeMain.routingTable.get(numberPrefixMatching).iterator();
                int i=0;
                while(itr.hasNext()){
                    NodeDetails nodeDetailsTemp = itr.next();

                    if(nodeDetailsTemp.getIdentifier()!=null) {
                        if (nodeDetailsTemp.getIdentifier().equals(closestMathHex)) {
                            numbericallyCloserIndex = i;
                            break;
                        }
                    }
                    i++;
                }

            }

        } else if (newIdentifierInt > allIdentifiers.get(allIdentifiers.size() - 1)) {
            //System.out.println("The value is greater than the last value in the list. ");
            System.out.println("compare - arrlastvalue " + (newIdentifierInt - arrLastValue) + " ((finishvalue-compare)+arrfirstvalue ) " + ((finalValue - newIdentifierInt) + arrFirstValue));

            if ((newIdentifierInt - arrLastValue) < ((finalValue - newIdentifierInt) + arrFirstValue)) {
                int closestMath = allIdentifiers.get(allIdentifiers.size() - 1);
                String closestMathHex = Integer.toHexString(closestMath);
                System.out.println("value closer to the last value in array .... " + allIdentifiers.get(allIdentifiers.size() - 1)+" In hex "+closestMathHex.toUpperCase());

                Iterator<NodeDetails> itr = nodeMain.routingTable.get(numberPrefixMatching).iterator();
                int i=0;
                while(itr.hasNext()){
                    NodeDetails nodeDetailsTemp = itr.next();

                    if(nodeDetailsTemp.getIdentifier()!=null) {
                        System.out.println("node detail temp " + nodeDetailsTemp.getIdentifier() + " closestMathhex " + closestMathHex);

                        if (nodeDetailsTemp.getIdentifier().equals(closestMathHex.toUpperCase())) {
                            numbericallyCloserIndex = i;
                            break;
                        }
                    }
                    i++;
                }


                System.out.println("numberCloserInde "+ numbericallyCloserIndex);
            } else {
                System.out.println("Equal or closer is the first value in the array ::: " + allIdentifiers.get(0));


                int closestMath = allIdentifiers.get(0);
                String closestMathHex = Integer.toHexString(closestMath);
                Iterator<NodeDetails> itr = nodeMain.routingTable.get(numberPrefixMatching).iterator();
                int i=0;
                while(itr.hasNext()){
                    NodeDetails nodeDetailsTemp = itr.next();

                    if(nodeDetailsTemp.getIdentifier()!=null) {
                        System.out.println("node detail temp " + nodeDetailsTemp.getIdentifier() + " closestMathhex 000" + closestMathHex);

                        if (nodeDetailsTemp.getIdentifier().equals(closestMathHex.toUpperCase())) {
                            numbericallyCloserIndex = i;
                            break;
                        }
                    }
                    i++;
                }

            }
        } else {
            System.out.println("The value falls between two nodes that are already present.");

            System.out.println(allIdentifiers.get(0)+" "+allIdentifiers.get(1)+" "+newIdentifierInt);
            int oldvalue = Math.abs(allIdentifiers.get(0) - newIdentifierInt);
            int index = 0;
            int newvale = 0;
            for (int i = 1; i < allIdentifiers.size(); i++) {
                newvale = Math.abs(allIdentifiers.get(i) - newIdentifierInt);

                if (newvale <= oldvalue) {
                    oldvalue = newvale;
                    index = i;

                }

            }
            numbericallyCloserIndex = index;
            System.out.println("New value "+newvale);
            System.out.println("oldvalue " + oldvalue + " index " + index + " value " + allIdentifiers.get(numbericallyCloserIndex));


        }

        return numbericallyCloserIndex;


    }*/

    private ArrayList<String> getAllIdentifiersInRow(ArrayList<NodeDetails> allNodesInRow){

        Iterator<NodeDetails> itr = allNodesInRow.iterator();
        ArrayList<String> allIdentifier = new ArrayList<String>();

        while(itr.hasNext()){
            NodeDetails tempNode = itr.next();
            if(tempNode.getIdentifier()!=null) {
                allIdentifier.add(tempNode.getIdentifier());
            }

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