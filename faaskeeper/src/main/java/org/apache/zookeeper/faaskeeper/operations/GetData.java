package org.apache.zookeeper.faaskeeper.operations;

public class GetData extends DirectOperation {

    public GetData(String sessionID, String path, Object watch) {
        super(sessionID, path, null);
    }

    public String getName() {
        return "get_data";
    }
}
