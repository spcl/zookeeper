package org.apache.zookeeper.faaskeeper.queue;

import org.apache.zookeeper.faaskeeper.model.ReadOpResult;
import java.util.concurrent.CompletableFuture;

public class CloudDirectResult extends EventQueueItem {
    public final int requestID;
    public final CompletableFuture<ReadOpResult> future;
    public final ReadOpResult result;

    public CloudDirectResult(int requestID, ReadOpResult result, CompletableFuture<ReadOpResult> future) {
        super(null);
        this.requestID = requestID;
        this.future = future;
        this.result = result;
    }

    public String getEventType() {
        return EventType.CLOUD_DIRECT_RESULT.getValue();
    }
}