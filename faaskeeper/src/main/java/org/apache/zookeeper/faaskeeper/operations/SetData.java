package org.apache.zookeeper.faaskeeper.operations;

import java.util.Map;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;

import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.faaskeeper.FaasKeeperClient;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.model.SystemCounter;
import org.apache.zookeeper.faaskeeper.model.Version;
import org.apache.zookeeper.KeeperException.Code;

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

                if (this.cb != null) {
                    Stat stat = new Stat();
                    FaasKeeperClient.updateStat(stat, n, null);
                    ((AsyncCallback.StatCallback)this.cb).processResult(Code.OK.intValue(), this.getPath(), this.callbackCtx, stat);
                }
            } catch (Exception e) {
                LOG.error("Error processing result: " + result.toString(), e);
                future.completeExceptionally(e);

                if (this.cb != null) {
                    ((AsyncCallback.StatCallback)this.cb).processResult(Code.SYSTEMERROR.intValue(), this.getPath(), this.callbackCtx, null);
                }
            }
        } else {
            String reason = result.get("reason") != null ? result.get("reason").asText() : "";
            Code code;
            switch (reason) {
                case "update_failure":
                    code = Code.SYSTEMERROR;
                    future.completeExceptionally(new RuntimeException("Update failure"));
                    break;
                case "node_doesnt_exist":
                    code = Code.NONODE;
                    future.completeExceptionally(new RuntimeException("Node doesn't exist"));
                    break;
                case "update_not_committed":
                    code = Code.SYSTEMERROR;
                    future.completeExceptionally(new RuntimeException("Update could not be applied"));
                    break;
                default:
                    code = Code.SYSTEMERROR;
                    future.completeExceptionally(new RuntimeException("Unknown error: " + reason));
                    break;
            }

            if (this.cb != null) {
                ((AsyncCallback.StatCallback)this.cb).processResult(code.intValue(), this.getPath(), this.callbackCtx, null);
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
