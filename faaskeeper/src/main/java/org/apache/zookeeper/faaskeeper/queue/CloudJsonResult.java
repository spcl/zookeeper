package org.apache.zookeeper.faaskeeper.queue;
import com.fasterxml.jackson.databind.JsonNode;

public class CloudJsonResult extends CloudIndirectResult {
    public final JsonNode result;
    public CloudJsonResult(JsonNode result) {
        super();
        this.result = result;
    }
}