package org.apache.zookeeper.faaskeeper.operations;

public class NodeExists extends DirectOperation {

    public NodeExists(String sessionID, String path, Object watch) {
        super(sessionID, path, null, null, null);
    }

    public String getName() {
        return "exists";
    }
}
