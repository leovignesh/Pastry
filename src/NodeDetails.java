
/**
 * Created by leo on 9/26/15.
 */
public class NodeDetails {

    private String ipAddress;
    private int port;
    private String nickName;
    private String identifier;

    public NodeDetails(String ipAddress,int port,String nickName,String identifier) {

        this.ipAddress = ipAddress;
        this.port = port;
        this.nickName = nickName;
        this.identifier = identifier;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getIpPort(){
        return this.ipAddress+":"+this.port;
    }

}
