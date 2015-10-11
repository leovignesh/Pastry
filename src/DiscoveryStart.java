import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by leo on 9/26/15.
 */
public class DiscoveryStart {


    private static int selfPort;

    private ServerSocket serverSocket=null;
    private Socket socket = null;


    public DiscoveryStart(int selfPort){
        this.selfPort = selfPort;

        try {
            serverSocket = new ServerSocket(selfPort);
        }catch (IOException e){
            e.printStackTrace();
            System.out.println("Exception occured when starting the server.");
        }

    }



    public void startDiscoveryNode() {

        DiscoveryClient discoveryClient = new DiscoveryClient();
        Thread thread1 = new Thread(discoveryClient);
        thread1.start();


        while (true) {

            try {
                System.out.println("Discovery Server Started....");
                socket = serverSocket.accept();
            }catch (IOException e){
                e.printStackTrace();
                System.out.println("Exception occured when starting accepting the server.");
            }

            Discovery discovery = new Discovery(socket);
            Thread thread =new Thread(discovery);
            thread.start();

        }
    }

    public static void main(String[] args) {

        if(args.length != 1){
            throw new IllegalArgumentException("Parameter(s): <Self Port>" );
        }

        DiscoveryStart discoveryStart = new DiscoveryStart(Integer.parseInt(args[0]));
        discoveryStart.startDiscoveryNode();
    }
}
