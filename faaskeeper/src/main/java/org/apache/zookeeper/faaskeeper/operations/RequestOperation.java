package org.apache.zookeeper.faaskeeper.operations;

import java.util.Map;
import org.apache.zookeeper.faaskeeper.queue.CloudErrorResult;
import org.apache.zookeeper.faaskeeper.queue.CloudJsonResult;


public abstract class RequestOperation extends Operation {
    public RequestOperation(String sessionId, String path) {
        super(sessionId, path);
    }

    // public RequestOperation(Map<String, Object> data) {
    //     super((String) data.get("sessionId"), (String) data.get("path"));
    // }

    public abstract Map<String, Object> generateRequest();

    public boolean isCloudRequest() {
        return true;
    }

    public abstract void processResult(CloudJsonResult event);

    public abstract void processError(CloudErrorResult event);
}