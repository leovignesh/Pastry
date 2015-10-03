import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by leo on 10/3/15.
 */
public class InitialRoutingTable {

    private String identifier;
    private String ipAddress;
    private int port;
    private String nickName;

    private NodeMain nodeMain;
    private NodeDetails nodeDetails;

    // Logger Initialization
    Logger log = Logger.getLogger(NodeMain.class);

    public InitialRoutingTable(NodeDetails nodeDetails,NodeMain nodeMain){

        this.nodeDetails = nodeDetails;
        this.identifier = nodeDetails.getIdentifier();
        this.ipAddress = nodeDetails.getIpAddress();
        this.port = nodeDetails.getPort();
        this.nickName = nodeDetails.getNickName();
        this.nodeMain = nodeMain;

    }

    public void buildInitialRoutingTable(){

        char[] charArray = splitIdentifierToCharArray(identifier);
        log.info("Number of chars in idenfitier : " + charArray.length);

        // create the initial

        nodeMain.routingTable.put(0, new ArrayList<NodeDetails>());
        nodeMain.routingTable.put(1, new ArrayList<NodeDetails>());
        nodeMain.routingTable.put(2, new ArrayList<NodeDetails>());
        nodeMain.routingTable.put(3, new ArrayList<NodeDetails>());


        Set<Integer> routingTableKey = nodeMain.routingTable.keySet();
        Iterator itr = routingTableKey.iterator();

        while (itr.hasNext()){
            int key = (Integer)itr.next();

            for(int i=0;i<16;i++){

                //log.info("Key value : "+key+" i value "+i+" char[] "+charArray[key]);
                // Convert Hex to int and compare and save
                if(i==Integer.parseInt(charArray[key] + "".trim(), 16)){
                    nodeMain.routingTable.get(key).add(nodeDetails);
                } else {
                    nodeMain.routingTable.get(key).add(new NodeDetails(null,0,null,null));
                }
            }

        }

    }

    private char[] splitIdentifierToCharArray(String identifier){
        return identifier.toCharArray();

    }

}
