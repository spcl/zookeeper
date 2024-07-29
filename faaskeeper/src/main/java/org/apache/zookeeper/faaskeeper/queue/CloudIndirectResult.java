package org.apache.zookeeper.faaskeeper.queue;
import java.util.concurrent.CompletableFuture;
import org.apache.zookeeper.faaskeeper.model.Node;

public abstract class CloudIndirectResult extends EventQueueItem {
    protected CompletableFuture<Node> future;
    
    public CloudIndirectResult() {
        super();
    }

    public void setFuture(CompletableFuture<Node> future) {
        this.future = future;
    }

    public CompletableFuture<Node> getFuture() {
        return future;
    }

    public String getEventType() {
        return EventType.CLOUD_INDIRECT_RESULT.getValue();
    }
}
