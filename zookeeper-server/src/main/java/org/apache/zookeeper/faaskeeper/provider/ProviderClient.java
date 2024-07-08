package org.apache.zookeeper.faaskeeper.provider;

import java.util.Map;
import java.util.Optional;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.FaasKeeperConfig;

// import java.util.List;
// import java.util.Optional;
// import java.util.AbstractMap.SimpleEntry;

public abstract class ProviderClient {

    protected static FaasKeeperConfig config;

    public ProviderClient(FaasKeeperConfig cfg) {
        config = cfg;
    }

    public abstract void registerSession(String sessionId, String sourceAddr, boolean heartbeat);

    public abstract void sendRequest(String requestId, Map <String, Object> data) throws Exception;

    public abstract Node getData(String path) throws Exception;

    // public abstract Watch registerWatch(Node node, WatchType watchType, WatchCallbackType watch, SimpleEntry<String, Integer> listenAddress);

    // public Object executeRequest(DirectOperation op, SimpleEntry<String, Integer> listenAddress) {
    //     if (op instanceof GetData) {
    //         return getData(op.getPath(), op.getWatch(), listenAddress);
    //     } else if (op instanceof ExistsNode) {
    //         return exists(op.getPath());
    //     } else if (op instanceof GetChildren) {
    //         return getChildren(op.getPath(), op.isIncludeData());
    //     } else if (op instanceof RegisterSession) {
    //         registerSession(op.getSessionId(), op.getSourceAddr(), op.isHeartbeat());
    //         return null;
    //     } else {
    //         throw new UnsupportedOperationException("Operation not supported");
    //     }
    // }
}
