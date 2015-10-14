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
        //log.debug("Thread Name : " + Thread.currentThread().getName());

        String mssRcvd=null;
        try {
            mssRcvd = receiveDataFromDestination(socket);
        }catch (IOException e){

        }
        //log.debug("Request received :  " + mssRcvd);
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
                //System.out.println("number of prefix matching "+numberPrefixMatching);
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

                boolean isSent = false;
                while(!isSent) {

                    String messToSend = null;
                    NodeDetails nodeDetailsToSend = lookup(newIdentifierRec);

                    int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(), newIdentifierRec);
                    char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(), newIdentifierRec);


                    // update the routing table.

                    Map<Integer, ArrayList<NodeDetails>> receivedTempRoutingTable = updateRoutingTable(tempRoutingTable, numberPrefixMatching);

                    if (nodeDetailsToSend.getIdentifier().equals(selfNodeDetails.getIdentifier())) {
                        //log.info("I am the guy responsible for the JOIN .YOU Find out where to place. Left or right.");
                        //log.info("Send JOIN OK");
                        messToSend = "FINALOK LEAF";

                        try {
                            nodeSocket = getNodeSocket(newIPRec, newPortRec);
                            sendDataToDestination(nodeSocket, messToSend);
                        } catch (IOException e) {
                            log.error("Exception occurred when trying to send JOINOK to Node.");
                            e.printStackTrace();
                        }

                        try {
                            sendObjectToDestination(nodeSocket, nodeMain.leafLeft);
                            sendObjectToDestination(nodeSocket, nodeMain.leafRight);
                            sendObjectToDestination(nodeSocket, selfNodeDetails);
                            sendObjectToDestination(nodeSocket, receivedTempRoutingTable);
                        } catch (IOException e) {
                            log.error("Exception occured when sending object output.");
                            e.printStackTrace();
                        }
                        isSent = true;

                        System.out.println("Path Travelled : " + pathTravelled);


                    } else {

                        pathTravelled = pathTravelled + selfNodeDetails.getIdentifier() + "=>";
                        messToSend = "JOIN " + ++hops + " " + newIPRec + " " + newPortRec + " " + newNickNameRec + " " + newIdentifierRec + " " + " " + pathTravelled;

                        try {
                            nodeSocket = getNodeSocket(nodeDetailsToSend.getIpAddress(), nodeDetailsToSend.getPort());
                            sendDataToDestination(nodeSocket, messToSend);
                            sendObjectToDestination(nodeSocket, receivedTempRoutingTable);
                            isSent = true;
                        } catch (IOException e) {

                            //nodeMain.routingTable.get()
                            log.error("Exception occurred when trying to send JOIN to Node. Node might be deleted. So remove it from the routing table and send the request again.");
                            
                            // Delete the entry from the routing table.
                            
                            for(int i=0;i<4;i++){
                            	
                            	for(int j=0;j<16;j++){
                            		
                            		if(nodeMain.routingTable.get(i).get(j).getIdentifier()!=null){
                            		
	                            		if(nodeMain.routingTable.get(i).get(j).getIdentifier().equals(nodeDetailsToSend.getIdentifier())){
	                            			
	                            			//log.info("Routing table entry prent for deleting . So removing it. "+nodeMain.routingTable.get(i).get(j).getIdentifier());
	                            			nodeMain.routingTable.get(i).set(j, returnNullNodeDetails());
	                            			
	                            		}
                            		}
                            		
                            	}
                            	
                            }
                            continue;
                            
                        }


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

                    String fileName = nodeMain.fileStoredDetails.get(hashIdentifier);
                    String nodeIdentifier = nodeDetails.getIdentifier();
                    String selfIdentifier = selfNodeDetails.getIdentifier();

                    String closestValue = closestMatch(hashIdentifier,nodeIdentifier,selfIdentifier);

                    // prepare to send the data.
                    File file = new File(fileName);
                    
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

                        //log.info("Send the file to the New node.");
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

                        //log.info(" File datastrucutre  before "+nodeMain.fileStoredDetails.size());
                        nodeMain.fileStoredDetails.remove(hashIdentifier);
                        
                        File fileToBeDeleted = new File("/tmp/hsperfdata_leovig"+fileName);
                        
                        fileToBeDeleted.delete();
                        //log.info(" File datastrucutre after "+nodeMain.fileStoredDetails.size());

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

        else if(requestType.equals("ROUTINGTABLECONV")){

            String rcvdIdentifier = tokens[1].trim();
            int rowNumber = Integer.parseInt(tokens[2].trim());

            try {

                ArrayList<NodeDetails> routingTable = (ArrayList<NodeDetails>)receiveObjectFromDestination(socket);

                log.info("ROUTING TABLE UPDATE RECEIVED FROM :  "+rcvdIdentifier);
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
            String filename = tokens[3].trim();
            String storeIPToSend = tokens[4].trim();
            int storePortToSend = Integer.parseInt(tokens[5].trim());
            String pathTravelled = tokens[6].trim();
            int hopCount = Integer.parseInt(tokens[7].trim());

            //log.info("Message received from "+storeIPToSend+" Port "+storePortToSend);

            //log.info("Message received to send the closest node for saving the file. Path Travelled "+pathTravelled);
            System.out.println("self node details "+selfNodeDetails.getIdentifier());
            
            boolean destinationFound=false;
            
            while(!destinationFound){
            
	            NodeDetails fileNodeDetails = lookup(identifierFile);
	            String messToSend = "";
	
	            if(typeOfOperation.equals("FILESAVE")){
	
	                if(fileNodeDetails.getIdentifier().equals(selfNodeDetails.getIdentifier())){
	                    //log.info("I am the guy responsible for the file. Send a message to the destination to send the file.");
	                    pathTravelled = pathTravelled+"=>"+selfNodeDetails.getIdentifier()+"=>FINAL";
	
	                    log.info("PATH TRAVELLED : "+pathTravelled);
	
	                    messToSend = "CLOSESTSERVER "+typeOfOperation+" "+fileNodeDetails.getIpAddress()+":"+fileNodeDetails.getPort()
	                            +" "+fileNodeDetails.getIdentifier()+" "+pathTravelled;
	
	                    try {
	                        nodeSocket = getNodeSocket(storeIPToSend, storePortToSend);
	                        destinationFound = true;
	                    } catch (IOException e) {
	                        System.out.println("Exception occued when trying to get datastore socket");
	                        e.printStackTrace();
	                    }
	
	                }else{
	                    //log.info("Forward the message to the closest node. ");
	                    //System.out.println("self node details --"+selfNodeDetails.getIdentifier());
	                    pathTravelled = pathTravelled+"=>"+selfNodeDetails.getIdentifier();
	                    System.out.println("PATH TRAVELLED : "+pathTravelled);
	
	                    messToSend = "GETNODE "+typeOfOperation+" "+identifierFile+" "+filename+" "+storeIPToSend+" "+storePortToSend+" "+pathTravelled+" "+ ++hopCount;
	                    try {
	                        nodeSocket = getNodeSocket(fileNodeDetails.getIpAddress(), fileNodeDetails.getPort());
	                        destinationFound = true;
	                    } catch (IOException e) {	
	                        
	                    	
	                    	//log.error("Exception occurred when trying to send FILESAVE to Node.");
	                    	log.error("Node NOT REACHABLE. So remove it from the routing table and send the request again.");
                            
                            // Delete the entry from the routing table.
                            
                            for(int i=0;i<4;i++){
                            	
                            	for(int j=0;j<16;j++){
                            		
                            		if(nodeMain.routingTable.get(i).get(j).getIdentifier()!=null){
	                            		if(nodeMain.routingTable.get(i).get(j).getIdentifier().equals(fileNodeDetails.getIdentifier())){
	                            			
	                            			//log.info("Routing table entry prent for deleting . So removing it. "+nodeMain.routingTable.get(i).get(j).getIdentifier());
	                            			nodeMain.routingTable.get(i).set(j, returnNullNodeDetails());
	                            			
	                            		}
                            		}
                            		
                            	}
                            	
                            }
	                    	
	                    	continue;
	                    }
	
	                }
	                try {

		                sendDataToDestination(nodeSocket,messToSend);

		            } catch (IOException e) {
		                System.out.println("Exception occured when trying to send the message to Node.");
		                e.printStackTrace();
		            }
	                
	                
	            }else if(typeOfOperation.equals("FILERET")){
	
	            	boolean filenotFound = false;
	            	if(fileNodeDetails.getIdentifier().equals(selfNodeDetails.getIdentifier())){
	                    //log.info("I am the guy responsible for sending the file.");
	                    pathTravelled = pathTravelled+"=>"+selfNodeDetails.getIdentifier()+"=>FINAL";
	
	                    log.debug("PATH TRAVELLED : "+pathTravelled);
	
	                    
	                    File file= null;
	                    FileInputStream fileInputStream =null;
	                    try{
	                        
		                	file = new File("/tmp/hsperfdata_leovig"+nodeMain.fileStoredDetails.get(identifierFile));
		                	fileInputStream = new FileInputStream(file);
		                	messToSend = "CLOSESTSERVER "+typeOfOperation+" "+fileNodeDetails.getIpAddress()+":"+fileNodeDetails.getPort()
		                            +" "+fileNodeDetails.getIdentifier()+" "+pathTravelled+" "+filename+" "+identifierFile+" FILEFOUND "+ ++hopCount;
		                	
		                	destinationFound = true;
		                	
	                    }catch(FileNotFoundException e){
	                    	
	                    	filenotFound = true;
	                    	messToSend = "CLOSESTSERVER "+ typeOfOperation+ " "+fileNodeDetails.getIpAddress()+":"+fileNodeDetails.getPort()
		                            +" "+fileNodeDetails.getIdentifier()+" "+pathTravelled+" "+filename+" "+identifierFile+" FILENOTFOUND " + ++hopCount;
	                    	log.error("File not found in the destination. So send message to the client. ");
	                    	
	                    }
	                    
	                    try {
	                        nodeSocket = getNodeSocket(storeIPToSend, storePortToSend);
	                        destinationFound = true;
	                        sendDataToDestination(nodeSocket,messToSend);
	                        
	                    } catch (IOException e) {
	                        System.out.println("Exception occued when trying to get datastore socket");
	                        e.printStackTrace();
	                    }
	                    
	                    
	                    
	                    // send the file to that guy if the file is found. If not dont go here.
	                    while(!filenotFound){
	                    
		                    int fileSize=0; 
		                	byte[] fileByte=null;
		                	
		                    try{
		                        
		                        fileSize = (int)file.length();
		                        fileByte = new byte[(int) fileSize];
		                        int numberRead = fileInputStream.read(fileByte, 0, fileSize);
		                        
		                        // Send the actual File.
		                        
		                        DataOutputStream dataOutputStream = new DataOutputStream(nodeSocket.getOutputStream());
		                        dataOutputStream.writeInt(fileSize);
		                        dataOutputStream.write(fileByte,0,fileSize);
		                        dataOutputStream.flush();
		                        filenotFound = true;
		                          
		                    }catch (IOException e){
		                        log.error("Exceptin occured when reading from a file.");
		                        e.printStackTrace();
		                        break;
		                    }
	                    }
	                    
	
	                }else{
	                    //log.info("Forward the Retreival message to the closest node. ");
	                    //System.out.println("self node details --"+selfNodeDetails.getIdentifier());
	                    pathTravelled = pathTravelled+"=>"+selfNodeDetails.getIdentifier();
	                    //System.out.println("Path travelled : "+pathTravelled);
	
	                    messToSend = "GETNODE "+typeOfOperation+" "+identifierFile+" "+filename+" "+storeIPToSend+" "+storePortToSend+" "+pathTravelled+" "+ ++hopCount;
	                    try {
	                        nodeSocket = getNodeSocket(fileNodeDetails.getIpAddress(), fileNodeDetails.getPort());
	                        destinationFound = true;
	                        
	                        sendDataToDestination(nodeSocket,messToSend);
	                        
	                    } catch (IOException e) {
	                        
	                    		                    	
	                    	/*log.error("Exception occurred when trying to send FILESAVE to Node.");
	                    	log.error("Node might be deleted. So remove it from the routing table and send the request again.");*/
                            
                            // Delete the entry from the routing table.
                            
                            for(int i=0;i<4;i++){
                            	
                            	for(int j=0;j<16;j++){
                            		
                            		if(nodeMain.routingTable.get(i).get(j).getIdentifier()!=null){
	                            		if(nodeMain.routingTable.get(i).get(j).getIdentifier().equals(fileNodeDetails.getIdentifier())){
	                            			
	                            			log.info("ROUTING TABLE ENTRY PRESET .DELETE IT "+nodeMain.routingTable.get(i).get(j).getIdentifier());
	                            			nodeMain.routingTable.get(i).set(j, returnNullNodeDetails());
	                            			
	                            		}
                            		}
                            		
                            	}
                            	
                            }
	                    	
	                    	
	                    	continue;
	                    	
	                    }
	
	                }
	            }
	            
	            

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
                //System.out.println("File name received "+fileName);
                //String newFileNameRcvd = "/tmp/hsperfdata_leovig"+fileName;
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
    
    private NodeDetails returnNullNodeDetails(){
    	
    	return new NodeDetails(null, 0, null, null);
    	
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
        Set<String> removeAddress = new HashSet<String>();

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

                            //System.out.println("preparing to send "+nodeDetailsTemp.getIdentifier());
                            nodeSocket = getNodeSocket(ipAddressTemp,portTemp);
                            sendDataToDestination(nodeSocket,messToSend);
                            sendObjectToDestination(nodeSocket,nodeMain.routingTable.get(i));
                            //System.out.println("Routing table information sent to "+nodeDetailsTemp.getIdentifier());

                            alreadySent.add(nodeDetailsTemp.getIdentifier());
                        } catch (IOException e) {
                            log.info("NODE DOWN. UPDATE ROUTING TABLE");
                            
                            // Remove it from my routing table.
                            removeAddress.add(nodeDetailsTemp.getIdentifier());
                            
                        }


                    }

                }


            }
        }
        
        if(removeAddress.size()!=0){
        	
        	Iterator<String> itr= removeAddress.iterator();
	    	while(itr.hasNext()){
	    		String tempValue = itr.next();
	    		
		    	System.out.println("Value to be removed . "+tempValue);
		        for(int i=0;i<4;i++){
		        	
		        	for(int j=0;j<16;j++){
		        		
		        		if(nodeMain.routingTable.get(i).get(j).getIdentifier().equals(tempValue)){
		        			
		        			//log.info("Remove routing table entry for sendin the routing table updates. "+nodeMain.routingTable.get(i).get(j).getIdentifier());
		        			nodeMain.routingTable.get(i).set(j, null);
		        			//log.info("Entry after deleing : "+nodeMain.routingTable.get(i).get(j).getIdentifier());
		        			
		        		}
		        		
		        	}
		        }
        	}
        	
        
        }


    }



    private Map<Integer,ArrayList<NodeDetails>>  updateRoutingTable(Map<Integer,ArrayList<NodeDetails>> routingTableTemp,int rowsMatching){

        //System.out.println("Updating routing table.");

        //System.out.println("Row number of current routing table : "+rowsMatching);

        for(int j=0;j<=rowsMatching;j++){

            for(int i=0;i<routingTableTemp.get(j).size();i++){

                if(routingTableTemp.get(j).get(i).getIdentifier()==null){
                    //System.out.println("Value is null ");

                    if(nodeMain.routingTable.get(j).get(i).getIdentifier()!=null){
                        //System.out.println("Here it is present... grab it ");
                        routingTableTemp.get(j).set(i,nodeMain.routingTable.get(j).get(i));
                    }

                }else{
                    //System.out.println("Value is present...");
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

        log.info("Right "+nodeMain.leafRight.getIdentifier()+" left "+nodeMain.leafLeft.getIdentifier()+" New ID "+newIdentifierRec);
        //log.info("right "+temprightLeaf+" left "+templeftLeaf+" chk "+tempRecvdNodeIdenfifier );

        boolean betweenleafs = true;
        NodeDetails  nodedetailsTemp = null;

        if(temprightLeaf!= templeftLeaf){
            betweenleafs = isBetween(temprightLeaf,templeftLeaf,tempRecvdNodeIdenfifier);
        }


        log.debug("Value falls betwene leafsets ? : "+betweenleafs);

        if(!betweenleafs){
            // chk the routing table for closest match.
            log.debug("Values fall between leafsets");
            int numberPrefixMatching = numberOfPreFixMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
            char firstNonMatchingPrefix = firstPreFixNotMatching(selfNodeDetails.getIdentifier(),newIdentifierRec);
            int firstNonMatchingPrefixInt = Integer.parseInt(firstNonMatchingPrefix+"",16);

            //log.info("Numer of prefix matching "+numberPrefixMatching+" firstnonmatching prefix "+firstNonMatchingPrefixInt);

            int index = firstNonMatchingPrefixInt;
            String messToSend =null;
            // Find the numerically closest guy from routing table and send it to that guy.
            if(nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt).getIdentifier()!=null){
                log.debug("NOTBETWEENLEWFS : There is an entry in the non matching cell. Forward to that guy.");
                nodedetailsTemp = nodeMain.routingTable.get(numberPrefixMatching).get(firstNonMatchingPrefixInt);
                //log.info("NOTBETWEENLEWFS : Messge to be sent to  "+nodedetailsTemp.getIdentifier());
            }else{

                log.info("No entry in the non matching cell. So check for closest.");
                index = getNumericallyCloserIndex(numberPrefixMatching,newIdentifierRec);
                //System.out.println("Closest index received "+index+" routing table "+nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier()+" self node "+selfNodeDetails.getIdentifier());

                if(nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier().equals(selfNodeDetails.getIdentifier())){

                    //log.info("Check if I am the closer or the leaf sets are closer.");
                    // Check the leaf set to see who is closer. 

                    String closestMatchLeafSet = closestMatch(newIdentifierRec,left,right);
                    String closestMatch = closestMatch(newIdentifierRec,closestMatchLeafSet,selfNodeDetails.getIdentifier());

                    
                    
                    if(closestMatch.equals(selfNodeDetails.getIdentifier())){
                        log.info("I am closet. Place it near me");
                        nodedetailsTemp = selfNodeDetails;
                    }else if(closestMatch.equals(left)){
                        log.info("LEFT CLOSER");
                        nodedetailsTemp = nodeMain.leafLeft;
                    }else if(closestMatch.equals(right)){
                        log.info("RIGHT CLOSER");
                        nodedetailsTemp = nodeMain.leafRight;
                    }


                }else{
                    log.info("Numerically closer : " + nodeMain.routingTable.get(numberPrefixMatching).get(index).getIdentifier());
                    //log.info("Have to send the packet to him. ");

                    nodedetailsTemp = nodeMain.routingTable.get(numberPrefixMatching).get(index);

                }
            }


        }else {
            log.info("Value falls between leafsets");

            String closestMatchLeafSet = closestMatch(newIdentifierRec,left,right);
            String closestMatch = closestMatch(newIdentifierRec,closestMatchLeafSet,selfNodeDetails.getIdentifier());
            
            //System.out.println("value of closest Math "+closestMatch);
            
            if(closestMatch.equals(selfNodeDetails.getIdentifier())){
                log.info("I AM CLOSER");
                nodedetailsTemp = selfNodeDetails;
            }else if(closestMatch.equals(left)){
                log.info("LEFT CLOSER");
                nodedetailsTemp = nodeMain.leafLeft;
            }else if(closestMatch.equals(right)){
                log.info("RIGHT CLOSER");
                nodedetailsTemp = nodeMain.leafRight;
            }
        }

        log.debug("CLOSEST MATH FOR : "+newIdentifierRec+" is : "+nodedetailsTemp.getIdentifier());

        return nodedetailsTemp;
    }

    private String closestMatch(String compareValue,String left,String right) {

        int leftNum = Integer.parseInt(left, 16);
        int rightNum = Integer.parseInt(right, 16);
        int compareValueNum = Integer.parseInt(compareValue, 16);

        //System.out.println(rightNum + " " + leftNum + " " + compareValueNum);

        int nodeLeft = leftNum - compareValueNum;
        int reversrLeft = nodeLeft - Integer.parseInt("FFFF", 16);
        int first = Math.min(Math.abs(nodeLeft), Math.abs(reversrLeft));


        int nodeRigh = rightNum - compareValueNum;
        int reversrRight = nodeRigh - Integer.parseInt("FFFF", 16);
        int second = Math.min(Math.abs(nodeRigh), Math.abs(reversrRight));

        //log.info("	First " + first + " Second " + second);

        if (first < second) {
        	
            return left;

        } else if (first > second) {
        	
        	return right;
        } else {
        	if (leftNum < rightNum) {
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