package org.apache.zookeeper.faaskeeper.queue;

import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.operations.RequestOperation;

public class CloudExpectedResult extends EventQueueItem {
    public final int requestID;
    public final RequestOperation op;
    public final CompletableFuture<Node> future;
    public CloudExpectedResult(int requestID, RequestOperation op, CompletableFuture<Node> future) {
        super();
        this.requestID = requestID;
        this.op = op;
        this.future = future;
    }

    public String getEventType() {
        return EventType.CLOUD_EXPECTED_RESULT.getValue();
    }
}