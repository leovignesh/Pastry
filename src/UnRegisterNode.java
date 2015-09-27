import java.net.Socket;

/**
 * Created by leo on 9/26/15.
 */
public class UnRegisterNode implements  Runnable{

    private Socket socket;
    private String ipAddress;
    private int port;
    private Discovery discovery;
    private String nickName;

    public UnRegisterNode(Socket socket,Discovery discovery,String ipAddress,int port,String nickName) {
        this.socket = socket;
        this.ipAddress = ipAddress;
        this.port=port;
        this.discovery=discovery;
        this.nickName=nickName;

    }


    @Override
    public void run() {



    }
}
