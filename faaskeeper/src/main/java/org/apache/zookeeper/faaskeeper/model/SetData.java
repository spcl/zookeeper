package org.apache.zookeeper.faaskeeper.model;

import java.util.Map;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.JsonNode;

public class SetData extends RequestOperation {
    private byte[] value;
    private int version;
    private String encodedValue;
    private static final Logger LOG;
    static {
        LOG = LoggerFactory.getLogger(SetData.class);
    }

    public SetData(String sessionId, String path, byte[] value, int version) {
        super(sessionId, path);
        this.value = value;
        this.version = version;
        this.encodedValue = Base64.getEncoder().encodeToString(value);
    }

    public Map<String, Object> generateRequest() {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("op", getName());
        requestData.put("path", this.path);
        requestData.put("session_id", this.sessionId);
        requestData.put("data", this.value);
        requestData.put("version", this.version);

        return requestData;
    }

    public void processResult(JsonNode result, CompletableFuture<Node> future) {
        if ("success".equals(result.get("status").asText())) {
            try {
                Node n = new Node(result.get("path").asText());
                JsonNode sysCounterNode = result.get("modified_system_counter");
                if (sysCounterNode.isArray()) {
                    List<BigInteger> sysCounter = new ArrayList<>();
                    for (JsonNode val: sysCounterNode) {
                        sysCounter.add(new BigInteger(val.asText()));
                    }
                    n.setModified(new Version(SystemCounter.fromRawData(sysCounter), null));
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
                case "update_failure":
                    future.completeExceptionally(new RuntimeException("Update failure"));
                    break;
                case "node_doesnt_exist":
                    future.completeExceptionally(new RuntimeException("Node doesn't exist"));
                    break;
                case "update_not_committed":
                    future.completeExceptionally(new RuntimeException("Update could not be applied"));
                    break;
                default:
                    future.completeExceptionally(new RuntimeException("Unknown error: " + reason));
                    break;
            }
        }
    }

    public String getName() {
        return "set_data";
    }

    public String getDataB64() {
        return encodedValue;
    }

    public void setDataB64(String val) {
        encodedValue = val;
    }
}
