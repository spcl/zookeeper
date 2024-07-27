package org.apache.zookeeper.faaskeeper.operations;

import java.util.Map;
import java.util.HashMap;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.JsonNode;

public class DeleteNode extends RequestOperation {
    private int version;
    private static final Logger LOG;
    static {
        LOG = LoggerFactory.getLogger(DeleteNode.class);
    }

    public DeleteNode(String sessionId, String path, int version) {
        super(sessionId, path);
        this.version = version;
    }

    public Map<String, Object> generateRequest() {
        Map<String, Object> requestData = new HashMap<>();
        requestData.put("op", getName());
        requestData.put("path", this.path);
        requestData.put("session_id", this.sessionId);
        requestData.put("version", this.version);

        return requestData;
    }

    public void processResult(JsonNode result, CompletableFuture<Node> future) {
        LOG.debug("Processing res: " + result.toString());

        int rc = Code.OK.intValue();

        if ("success".equals(result.get("status").asText())) {
            future.complete(null);
        } else {
            String reason = result.get("reason") != null ? result.get("reason").asText() : "";
            switch (reason) {
                case "update_failure":
                    future.completeExceptionally(new RuntimeException("Update failure"));
                    rc = Code.SYSTEMERROR.intValue();
                    break;
                case "node_doesnt_exist":
                    future.completeExceptionally(new KeeperException.NoNodeException(result.get("path").asText()));

                    rc = Code.NONODE.intValue();
                    break;
                case "update_not_committed":
                    future.completeExceptionally(new RuntimeException("Update could not be committed"));
                    rc = Code.SYSTEMERROR.intValue();
                    break;
                case "not_empty":
                    future.completeExceptionally(new KeeperException.NotEmptyException(result.get("path").asText()));
                    rc = Code.NOTEMPTY.intValue();
                    break;
                default:
                    future.completeExceptionally(new RuntimeException("Unknown error: " + reason));
                    rc = Code.SYSTEMERROR.intValue();
                    break;
            }
        }

        if (this.cb != null) {
            ((AsyncCallback.VoidCallback)this.cb).processResult(rc, this.getPath(), this.callbackCtx);
        }
    }

    public String getName() {
        return "delete_node";
    }
}

