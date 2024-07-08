package org.apache.zookeeper.faaskeeper.model;

public class NodeExists extends DirectOperation {

    public NodeExists(String sessionID, String path, Object watch) {
        super(sessionID, path, null);
    }

    public String getName() {
        return "exists";
    }
}
