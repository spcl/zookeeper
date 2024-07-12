package org.apache.zookeeper.faaskeeper.queue;

import java.util.concurrent.CompletableFuture;

import org.apache.zookeeper.faaskeeper.operations.ReadOpResult;

public class CloudDirectResult extends EventQueueItem {
    public final int requestID;
    public final CompletableFuture<ReadOpResult> future;
    public final ReadOpResult result;

    public CloudDirectResult(int requestID, ReadOpResult result, CompletableFuture<ReadOpResult> future) {
        super();
        this.requestID = requestID;
        this.future = future;
        this.result = result;
    }

    public String getEventType() {
        return EventType.CLOUD_DIRECT_RESULT.getValue();
    }
}