import org.apache.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;

/**
 * Created by leo on 10/3/15.
 */
public class NodeClient implements  Runnable{

    private NodeMain nodeMain;

    // Logger Initialization
    Logger log = Logger.getLogger(NodeMain.class);

    public NodeClient(NodeMain nodeMain){
        this.nodeMain = nodeMain;
    }


    @Override
    public void run() {
        startClient();
    }

    private void startClient(){

        while (true){

            System.out.println("***********Please enter the option************");
            System.out.println("1, Display the Routing table");
            System.out.println("2, Display the Leaf Set");
            System.out.println("3, List the files in the Node");
            System.out.println("4, Leave the Distributed system.");

            Scanner scanner = new Scanner(System.in);

            int input =0;

            try{
                do{
                    System.out.println("Enter a Number 1 or 2");
                    while (!scanner.hasNextInt()){
                        scanner.next();
                        System.out.println("Please enter a valid number");
                    }
                    input = scanner.nextInt();
                }while (input<1 || input>3);
            }catch (Exception e){
                System.out.println("Enter a valid Number");
            }

            switch (input) {
                case 1:
                    System.out.println("******************\n");
                    System.out.println("Routing table Entries :\n");

                    Set<Integer> routingTableKey = nodeMain.routingTable.keySet();
                    Iterator itr = routingTableKey.iterator();

                    while (itr.hasNext()) {
                        int index = (Integer) itr.next();

                        int entries = nodeMain.routingTable.get(index).size();


                        System.out.println(nodeMain.routingTable.get(index).get(0).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(1).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(2).getIdentifier()
                                +" | "+nodeMain.routingTable.get(index).get(3).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(4).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(5).getIdentifier()
                                +" | "+nodeMain.routingTable.get(index).get(6).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(7).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(8).getIdentifier()
                                +" | "+nodeMain.routingTable.get(index).get(9).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(10).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(11).getIdentifier()
                                +" | "+nodeMain.routingTable.get(index).get(12).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(13).getIdentifier() +" | "+nodeMain.routingTable.get(index).get(14).getIdentifier()
                                +" | "+nodeMain.routingTable.get(index).get(15).getIdentifier() +"\n");
                    }
                    System.out.println("*****************");
                    break;
                case 2:
                    System.out.println("************");
                    System.out.println("Display the Leaf Set");

                    System.out.println("Left Leaf IPAddress : " + nodeMain.leafLeft.getIpAddress() + " : " + nodeMain.leafLeft.getPort() + "  " + nodeMain.leafLeft.getIdentifier() + "  " + nodeMain.leafLeft.getNickName());
                    System.out.println("Right Leaf IPAddress : " + nodeMain.leafRight.getIpAddress() + " : " + nodeMain.leafRight.getPort() + "  " + nodeMain.leafRight.getIdentifier() + "  " + nodeMain.leafRight.getNickName());
                    break;


                case 3:
                    System.out.println("***************");
                    System.out.println("List the files in the Node.");

                    Set<String> keys = nodeMain.fileStoredDetails.keySet();
                    Iterator<String> itr1 = keys.iterator();
                    int i=1;
                    while(itr1.hasNext()){
                        String keyvalue = itr1.next();
                        System.out.println(i+", "+keyvalue+" : "+nodeMain.fileStoredDetails.get(keyvalue));

                    }

                    break;

                case 4:
                	System.out.println("************");
                	System.out.println("Leave the distributed System");
                	boolean unRegStatus = nodeMain.unRegisterNode();

                    String ipAddressLeft = nodeMain.leafLeft.getIpAddress();
                    int portLeft = nodeMain.leafLeft.getPort();

                    String ipAddressRight = nodeMain.leafRight.getIpAddress();
                    int portRight = nodeMain.leafRight.getPort();
                
                	if(unRegStatus){
                		log.info("UnRegistration Successful. Inform the leaf nodes about leaving the network");

                		try {
                			String messToSend = "REMNODELEAFUPDATE 1";
							Socket nodeSocket = getNodeSocket(ipAddressLeft, portLeft);
							sendDataToDestination(nodeSocket, messToSend);
                            sendObjectToDestination(nodeSocket, nodeMain.leafRight);

                        } catch (IOException e) {
                            log.error("Exception occured when trying to send removal message.");
                            e.printStackTrace();
						}

                        try{
                            String messToSend = "REMNODELEAFUPDATE 0";
                            Socket nodeSocket1 = getNodeSocket(ipAddressRight, portRight);
                            sendDataToDestination(nodeSocket1, messToSend);
                            sendObjectToDestination(nodeSocket1, nodeMain.leafLeft);

                        }catch (IOException e){
                            log.error("Exception occured when trying to send messaget to destingation");
                            e.printStackTrace();
                        }

                	}

                    // Find the files in the system and disperse it.
                    Set<String> fileKeys = nodeMain.fileStoredDetails.keySet();


                    if(fileKeys.size()==0){
                        System.out.println("No files stored in the system.");
                    }else{

                        Iterator<String> itr2 = fileKeys.iterator();
                        while (itr2.hasNext()){
                            String hashIdentifier = itr2.next();

                            String leftNodeIdentifier = nodeMain.leafLeft.getIdentifier();
                            String rightNodeIdentifier = nodeMain.leafRight.getIdentifier();

                            String closestValue = closestMatch(hashIdentifier,leftNodeIdentifier,rightNodeIdentifier);

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
                                log.error("Exception occured when trying to copy the bytes.");
                                e.printStackTrace();
                            }

                            String messToSend = "FILEDATA "+nodeMain.fileStoredDetails.get(hashIdentifier)+" "+hashIdentifier+" "+fileSize;


                            if(closestValue.equals(leftNodeIdentifier)){

                                log.info("Send the file to the left Node.");
                                try {
                                    Socket nodeSocket = getNodeSocket(ipAddressLeft, portLeft);
                                    sendDataToDestination(nodeSocket, messToSend);

                                    // Send the files to the left node.

                                    DataOutputStream dataOutputStream = new DataOutputStream(nodeSocket.getOutputStream());
                                    dataOutputStream.writeInt(fileSize);
                                    dataOutputStream.write(fileByte,0,fileSize);
                                    dataOutputStream.flush();

                                } catch (IOException e) {
                                    log.error("Exception occured when trying to send removal message.");
                                    e.printStackTrace();
                                }

                            }else if(closestValue.equals(rightNodeIdentifier)){

                                log.info("Send the file to the right node.");

                                try{
                                    Socket nodeSocket1 = getNodeSocket(ipAddressRight, portRight);
                                    sendDataToDestination(nodeSocket1, messToSend);

                                    // send the files to right nodes.

                                    DataOutputStream dataOutputStream = new DataOutputStream(nodeSocket1.getOutputStream());
                                    dataOutputStream.writeInt(fileSize);
                                    dataOutputStream.write(fileByte,0,fileSize);
                                    dataOutputStream.flush();

                                }catch (IOException e){
                                    log.error("Exception occured when trying to send messaget to destingation");
                                    e.printStackTrace();
                                }
                            }

                        }


                    }

                    break;


            }

        }


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
