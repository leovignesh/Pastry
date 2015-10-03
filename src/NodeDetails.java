/**
 * Created by leo on 10/3/15.
 */
public class NodeDetails {

    private String identifier;
    private String ipAddress;
    private int port;
    private String nickName;

    public NodeDetails(String ipAddress,int port,String identifier,String nickName){
        this.identifier = identifier;
        this.ipAddress = ipAddress;
        this.port = port;
        this.nickName = nickName;
    }

    public String getIdentifier() {
        return identifier;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public int getPort() {
        return port;
    }

    public String getNickName() {
        return nickName;
    }
}
