import org.apache.log4j.Logger;

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

            Scanner scanner = new Scanner(System.in);

            int input =0;

            try{
                do{
                    System.out.println("Enter a Number 1 or 2");
                    while (!scanner.hasNextInt()){
                        System.out.println("Please enter a valid number");
                    }
                    input = scanner.nextInt();
                }while (input<1 || input>2);
            }catch (Exception e){
                System.out.println("Enter a valid Number");
            }

            switch (input) {
                case 1:
                    System.out.println("******************");
                    System.out.println("Display the Routing table entries");

                    Set<Integer> routingTableKey = nodeMain.routingTable.keySet();
                    Iterator itr = routingTableKey.iterator();

                    while (itr.hasNext()) {
                        int index = (Integer) itr.next();

                        int entries = nodeMain.routingTable.get(index).size();

                        System.out.println("\n Row [" + index + "] : ");
                        for (int i = 0; i < entries; i++) {

                            System.out.println("[" + index + "] [" + i + "] " + nodeMain.routingTable.get(index).get(i).getIpAddress() + " " + nodeMain.routingTable.get(index).get(i).getPort() + " " + nodeMain.routingTable.get(index).get(i).getIdentifier() + " " + nodeMain.routingTable.get(index).get(i).getNickName());

                        }
                    }
                    break;
                case 2:
                    System.out.println("************");
                    System.out.println("Display the Leaf Set");

                    System.out.println("Left Leaf IPAddress : " + nodeMain.leafLeft.getIpAddress() + " : " + nodeMain.leafLeft.getPort() + "  " + nodeMain.leafLeft.getIdentifier() + "  " + nodeMain.leafLeft.getNickName());
                    System.out.println("Right Leaf IPAddress : " + nodeMain.leafRight.getIpAddress() + " : " + nodeMain.leafRight.getPort() + "  " + nodeMain.leafRight.getIdentifier() + "  " + nodeMain.leafRight.getNickName());
                    break;
            }

        }


    }
}
