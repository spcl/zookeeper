package org.apache.zookeeper.faaskeeper.operations;

import java.util.Map;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.model.SystemCounter;
import org.apache.zookeeper.faaskeeper.model.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.ArrayList;
import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.JsonNode;

public class CreateNode extends RequestOperation {
    private byte[] value;
    private int flags;
    private static final Logger LOG;
    static {
        LOG = LoggerFactory.getLogger(CreateNode.class);
    }

    public CreateNode(String sessionId, String path, byte[] value, int flags, AsyncCallback cb, Object callbackCtx) {
        super(sessionId, path, cb, callbackCtx);
        this.value = value;
        this.flags = flags;
    }

    // public CreateNode(Map<String, Object> data) {
    //     super(data);
    //     this.value = (byte[]) data.get("data");
    //     this.flags = (int) data.get("flags");
    // }

    public Map<String, Object> generateRequest() {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("op", getName());
        requestData.put("path", this.path);
        requestData.put("session_id", this.sessionId);
        requestData.put("version", -1);
        // FIXME: Handle flags in FK. 0 is passed  as flag instead of actual value
        requestData.put("flags", 0);
        requestData.put("data", this.value);

        return requestData;
    }

    public void processResult(JsonNode result, CompletableFuture<Node> future) {
        String status = result.get("status").asText();
        if ("success".equals(status)) {
            try {
                Node n = new Node(result.get("path").asText());
                JsonNode sysCounterNode = result.get("system_counter");
                if (sysCounterNode.isArray()) {
                    List<BigInteger> sysCounter = new ArrayList<>();
                    for (JsonNode val: sysCounterNode) {
                        sysCounter.add(new BigInteger(val.asText()));
                    }
                    n.setCreated(new Version(SystemCounter.fromRawData(sysCounter), null));
                } else {
                    throw new IllegalStateException("System counter data is not an array");
                }
                future.complete(n);
            } catch (Exception e) {
                LOG.error("Error processing result: " + result.toString(), e);
                future.completeExceptionally(e);
            }
        } else {
            String reason = result.get("reason") != null ? result.get("reason").asText() : "";
            switch (reason) {
                case "node_exists":
                    future.completeExceptionally(new RuntimeException("Node already exists: " + result.get("path").asText()));
                    break;
                case "node_doesnt_exist":
                    future.completeExceptionally(new RuntimeException("Node does not exist: " + result.get("path").asText()));
                    break;
                case "update_not_committed":
                    future.completeExceptionally(new RuntimeException("Update could not be applied"));
                    break;
                default:
                    future.completeExceptionally(new RuntimeException("Unknown error occurred: " + reason));
            }
        }
    }

    public String getName() {
        return "create_node";
    }

}