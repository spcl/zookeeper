package org.apache.zookeeper.faaskeeper.operations;

import java.util.Map;

import org.apache.zookeeper.AsyncCallback;

public abstract class DirectOperation extends Operation {
    // TODO: define watch datatype
    private Object watch;
    
    public DirectOperation(String sessionId, String path, Object watch, AsyncCallback cb, Object callbackCtx) {
        super(sessionId, path, cb, callbackCtx);
        // TODO: Use actual value
        watch = null;

    }

    // public DirectOperation(Map<String, Object> data) {
    //     super((String) data.get("sessionId"), (String) data.get("path"));
    //     watch = data.get("watch");
    // }

    public boolean isCloudRequest() {
        return false;
    }

    public Object getWatch() {
        return watch;
    }
}
