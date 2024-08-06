package org.apache.zookeeper.faaskeeper.operations;

import java.util.Map;
import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.faaskeeper.FaasKeeperClient;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.model.SystemCounter;
import org.apache.zookeeper.faaskeeper.model.Version;
import org.apache.zookeeper.faaskeeper.queue.CloudErrorResult;
import org.apache.zookeeper.faaskeeper.queue.CloudJsonResult;
import org.apache.zookeeper.faaskeeper.queue.CloudProviderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.ArrayList;
import java.math.BigInteger;
import org.apache.zookeeper.KeeperException;
import java.util.concurrent.CompletableFuture;
import com.fasterxml.jackson.databind.JsonNode;

public class CreateNode extends RequestOperation {
    private byte[] value;
    private int flags;
    private static final Logger LOG;
    static {
        LOG = LoggerFactory.getLogger(CreateNode.class);
    }

    public CreateNode(String sessionId, String path, byte[] value, int flags) {
        super(sessionId, path);
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
        requestData.put("flags", this.flags);
        requestData.put("data", this.value);

        return requestData;
    }


    public void processResult(CloudJsonResult event) {
        JsonNode result = event.result;
        CompletableFuture<Node> future = event.getFuture();
        
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

                n.setData(this.value);

                if (this.cb != null) {
                    if (this.cb instanceof AsyncCallback.StringCallback) {
                        LOG.debug("Invoking createNode string callback");
                        // TODO Handle this case: If node is sequential, then Znode Path and Znode Name will be diff
                        ((AsyncCallback.StringCallback)this.cb).processResult(Code.OK.intValue(), this.getPath(), this.callbackCtx, n.getPath());
                    } else if (this.cb instanceof AsyncCallback.Create2Callback) {
                        LOG.debug("Invoking createNode Create2Callback");

                        Stat stat = new Stat();
                        FaasKeeperClient.updateStat(stat, n, CreateMode.fromFlag(this.flags));
                        
                        ((AsyncCallback.Create2Callback)this.cb).processResult(Code.OK.intValue(), this.getPath(), this.callbackCtx, n.getPath(), stat);
                    }
                }
                future.complete(n);
            } catch (Exception e) {
                LOG.error("Error processing result: " + result.toString(), e);
                future.completeExceptionally(e);
            }
        } else {
            String reason = result.get("reason") != null ? result.get("reason").asText() : "";
            int errorCode;
            switch (reason) {
                case "node_exists":
                    errorCode = Code.NODEEXISTS.intValue();
                    // future.completeExceptionally(new RuntimeException("Node already exists: " + result.get("path").asText()));
                    future.completeExceptionally(new KeeperException.NodeExistsException(result.get("path").asText()));
                    break;
                case "node_doesnt_exist":
                    errorCode = Code.NONODE.intValue();
                    future.completeExceptionally(new KeeperException.NoNodeException(result.get("path").asText()));
                    // future.completeExceptionally(new RuntimeException("Node does not exist: " + result.get("path").asText()));
                    break;
                case "update_not_committed":
                    errorCode = Code.SYSTEMERROR.intValue();
                    future.completeExceptionally(new RuntimeException("Update could not be committed"));
                    break;
                default:
                    errorCode = Code.SYSTEMERROR.intValue();
                    LOG.error("Unknown error type in create node: " + reason);
                    future.completeExceptionally(new RuntimeException("Unknown error occurred: " + reason));
            }

            if (this.cb != null) {
                if (this.cb instanceof AsyncCallback.StringCallback) {
                    LOG.debug("Invoking createNode string callback");
                    ((AsyncCallback.StringCallback)this.cb).processResult(errorCode, this.getPath(), this.callbackCtx, null);
                } else if (this.cb instanceof AsyncCallback.Create2Callback) {
                    LOG.debug("Invoking createNode Create2Callback");
                    ((AsyncCallback.Create2Callback)this.cb).processResult(errorCode, this.getPath(), this.callbackCtx, null, null);
                }
            }
        }
    }

    public void processError(CloudErrorResult event) {
        CompletableFuture<Node> future = event.getFuture();
        future.completeExceptionally(event.cloudException);
        
        if (this.cb != null) {
            if (this.cb instanceof AsyncCallback.StringCallback) {
                LOG.debug("Invoking createNode string callback");
                ((AsyncCallback.StringCallback)this.cb).processResult(Code.SYSTEMERROR.intValue(), this.getPath(), this.callbackCtx, null);
            } else if (this.cb instanceof AsyncCallback.Create2Callback) {
                LOG.debug("Invoking createNode Create2Callback");
                ((AsyncCallback.Create2Callback)this.cb).processResult(Code.SYSTEMERROR.intValue(), this.getPath(), this.callbackCtx, null, null);
            }
        }
    }

    

    public String getName() {
        return "create_node";
    }

}