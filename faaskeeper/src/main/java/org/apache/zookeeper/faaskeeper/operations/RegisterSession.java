package org.apache.zookeeper.faaskeeper.operations;

public class RegisterSession extends DirectOperation {
    public final String sourceAddr;
    public final boolean heartbeat;

    public RegisterSession(String sessionID, String sourceAddr, boolean heartbeat) {
        super(sessionID, "", null, null, null);
        this.sourceAddr = sourceAddr;
        this.heartbeat = heartbeat;
    }

    public String getName() {
        return "register_session";
    }
}
