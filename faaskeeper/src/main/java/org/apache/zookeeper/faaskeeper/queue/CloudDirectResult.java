package org.apache.zookeeper.faaskeeper.queue;

import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.faaskeeper.operations.DirectOperation;
import org.apache.zookeeper.faaskeeper.operations.ReadOpResult;

public class CloudDirectResult extends EventQueueItem {
    public final int requestID;
    public final CompletableFuture<ReadOpResult> future;
    public final ReadOpResult result;
    public final DirectOperation op;

    public CloudDirectResult(int requestID, ReadOpResult result, CompletableFuture<ReadOpResult> future, DirectOperation op) {
        super();
        this.requestID = requestID;
        this.future = future;
        this.result = result;
        this.op = op;
    }

    public String getEventType() {
        return EventType.CLOUD_DIRECT_RESULT.getValue();
    }
}