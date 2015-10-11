import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by leo on 10/10/15.
 */
public class StoreDataStart {

    private int selfPort;
    private String discoveryIP;
    private int discoveryPort;

    private ServerSocket serverSocket=null;
    private Socket socket = null;

    public StoreDataStart(int selfPort,String discoveryIP,int discoveryPort){
        this.selfPort = selfPort;
        this.discoveryIP = discoveryIP;
        this.discoveryPort = discoveryPort;

        try{
            serverSocket = new ServerSocket(selfPort);

        }catch (IOException e){
            System.out.println("Exception occured when trying to start connection.");
            e.printStackTrace();
        }

    }


    public void startStoreData(){

        StoreDataClient storeDataClient = new StoreDataClient(selfPort,discoveryIP,discoveryPort);
        Thread thread = new Thread(storeDataClient);
        thread.start();

        while (true){
            try {
                System.out.println("Store DataServer Started....");
                socket = serverSocket.accept();
            }catch (IOException e){
                System.out.println("Exception occured when trying to start datastore");
                e.printStackTrace();
            }
            System.out.println("Connection coming. ");

            StoreDataServer storeDataServer = new StoreDataServer(socket);
            Thread thread1 = new Thread(storeDataServer);
            thread1.start();

        }


    }


    public static void main(String[] args) {

        if(args.length != 1){
            throw new IllegalArgumentException("Parameter(s): <Self Port><Discovery IP ><Discovery Port>" );
        }

        StoreDataStart storeDataStart = new StoreDataStart(Integer.parseInt(args[0]),args[1].trim(),Integer.parseInt(args[2].trim()));
        storeDataStart.startStoreData();
    }



}
