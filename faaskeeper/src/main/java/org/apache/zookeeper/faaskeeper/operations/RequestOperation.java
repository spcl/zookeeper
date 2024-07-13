package org.apache.zookeeper.faaskeeper.operations;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.faaskeeper.model.Node;

import com.fasterxml.jackson.databind.JsonNode;

public abstract class RequestOperation extends Operation {
    public RequestOperation(String sessionId, String path, AsyncCallback cb, Object callbackCtx) {
        super(sessionId, path, cb, callbackCtx);
    }

    // public RequestOperation(Map<String, Object> data) {
    //     super((String) data.get("sessionId"), (String) data.get("path"));
    // }

    public abstract Map<String, Object> generateRequest();

    public boolean isCloudRequest() {
        return true;
    }

    public abstract void processResult(JsonNode result, CompletableFuture<Node> future);
}