package org.apache.zookeeper.faaskeeper.model;

public class RegisterSessionResult extends ReadOpResult {
    private String sessionID;

    public RegisterSessionResult(String sessionID) {
        this.sessionID = sessionID;
    }

    public String getSession() {
        return sessionID;
    }
}
