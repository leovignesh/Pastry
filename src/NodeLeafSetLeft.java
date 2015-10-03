/**
 * Created by leo on 10/2/15.
 */
public class NodeLeafSetLeft {


    private String leafsetLeftIdentifier;
    private String ipAddress;
    private int port;
    private String nickName;

    public String getNickName() {
        return nickName;
    }

    public void setNickName(String nickName) {
        this.nickName = nickName;
    }

    private NodeLeafSetLeft(String leafsetLeft){

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

    public String getLeafsetLeftIdentifier() {
        return leafsetLeftIdentifier;
    }

    public void setLeafsetLeftIdentifier(String leafsetLeftIdentifier) {
        this.leafsetLeftIdentifier = leafsetLeftIdentifier;
    }
}
