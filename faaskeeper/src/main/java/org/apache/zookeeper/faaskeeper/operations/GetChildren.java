package org.apache.zookeeper.faaskeeper.operations;

public class GetChildren extends DirectOperation {

    public GetChildren(String sessionID, String path, Object watch) {
        super(sessionID, path, null);
    }

    public String getName() {
        return "get_children";
    }
}
