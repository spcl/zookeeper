package org.apache.zookeeper.faaskeeper.queue;
import com.fasterxml.jackson.databind.JsonNode;

public class CloudIndirectResult extends EventQueueItem {
    public final JsonNode result;
    public CloudIndirectResult(JsonNode result) {
        super();
        this.result = result;
    }

    public String getEventType() {
        return EventType.CLOUD_INDIRECT_RESULT.getValue();
    }
}