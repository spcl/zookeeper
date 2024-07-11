package org.apache.zookeeper.faaskeeper;

import java.util.Map;
import java.util.HashMap;
import java.util.UUID;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.faaskeeper.queue.EventQueue;
import org.apache.zookeeper.faaskeeper.queue.WorkQueue;
import org.apache.zookeeper.faaskeeper.thread.SorterThread;
import org.apache.zookeeper.faaskeeper.thread.SqsListener;
import org.apache.zookeeper.faaskeeper.thread.SubmitterThread;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.faaskeeper.provider.ProviderClient;
import org.apache.zookeeper.faaskeeper.provider.AwsClient;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.model.NodeExists;
import org.apache.zookeeper.faaskeeper.model.ReadOpResult;
import org.apache.zookeeper.faaskeeper.model.SetData;
import org.apache.zookeeper.faaskeeper.model.GetData;
import org.apache.zookeeper.faaskeeper.model.GetDataResult;
import org.apache.zookeeper.faaskeeper.model.CreateNode;
import org.apache.zookeeper.faaskeeper.model.DeleteNode;
import org.apache.zookeeper.faaskeeper.model.GetChildren;
import org.apache.zookeeper.faaskeeper.model.GetChildrenResult;
import org.apache.zookeeper.faaskeeper.model.RegisterSession;

public class FaasKeeperClient {
    private FaasKeeperConfig cfg;
    private boolean heartbeat = true;
    private String sessionId;
    private ProviderClient providerClient;
    private SqsListener responseHandler;
    private SubmitterThread workThread;
    private SorterThread sorterThread;
    private EventQueue eventQueue;
    private WorkQueue workQueue;
    private static Map<String, Class<? extends ProviderClient>> providers = new HashMap<>();
    private static final Logger LOG;
    static {
        LOG = LoggerFactory.getLogger(FaasKeeperClient.class);
        providers.put(CloudProvider.serialize(CloudProvider.AWS), AwsClient.class);
    }

    public FaasKeeperClient(FaasKeeperConfig cfg, boolean heartbeat) {
        try {
            this.cfg = cfg;
            this.heartbeat = heartbeat;
            Class<? extends ProviderClient> providerClass = providers.get(CloudProvider.serialize(this.cfg.getCloudProvider()));
            this.providerClient = providerClass.getDeclaredConstructor(FaasKeeperConfig.class).newInstance(this.cfg);
            this.eventQueue = new EventQueue();
            this.workQueue = new WorkQueue();
        } catch (Exception e) {
            LOG.error("Error in initializing provider client", e);
            throw new RuntimeException("Error in initializing provider client", e);
        }
    }

    public String start() throws Exception {
        LOG.info("Starting FK connection");
        responseHandler = new SqsListener(eventQueue, cfg);
        sessionId = UUID.randomUUID().toString().substring(0, 8);

        workThread = new SubmitterThread(workQueue, eventQueue, providerClient, sessionId);
        sorterThread = new SorterThread(eventQueue);

        RegisterSession requestOp = new RegisterSession(sessionId, "", this.heartbeat);
        CompletableFuture<ReadOpResult> future = new CompletableFuture<ReadOpResult>();
        workQueue.addRequest(requestOp, future);
        future.get();

        LOG.info("Connection successful. sessionID = " + sessionId);
        return sessionId;
    }

    public void stop() throws Exception {
        // TODO: deregister session
        LOG.info("Closing FK connection");
        workQueue.close();
        workQueue.waitClose(5, 1);
        eventQueue.close();
        responseHandler.stop();
        workThread.stop();
        sorterThread.stop();
    }

    public static FaasKeeperClient buildClient(String configFilePath, boolean heartbeat) throws Exception {
        try {
            FaasKeeperConfig cfg = FaasKeeperConfig.buildFromConfigJson(configFilePath);
            return new FaasKeeperClient(cfg, heartbeat);
        } catch (Exception e) {
            LOG.error("Error in creating client", e);
            throw e;
        }
    }

    // flags represents createmode in its bit representation
    public String create(String path, byte[] value, int flags) throws Exception {
        CompletableFuture<Node> future = createAsync(path, value, flags);
        Node n = future.get();
        return n.getPath();
    }

    public CompletableFuture<Node> createAsync(String path, byte[] value, int flags) throws Exception {
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Missing session id in FK client");
        }
        CreateNode requestOp = new CreateNode(sessionId, path, value, flags);
        CompletableFuture<Node> future = new CompletableFuture<Node>();
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public Node setData(String path, byte[] value, int version) throws Exception {
        CompletableFuture<Node> future = setDataAsync(path, value, version);
        return future.get();
    }

    public CompletableFuture<Node> setDataAsync(String path, byte[] value, int version) throws Exception {
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Missing session id in FK client");
        }
        SetData requestOp = new SetData(sessionId, path, value, version);
        CompletableFuture<Node> future = new CompletableFuture<Node>();
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public void delete(String path, int version) throws Exception {
        CompletableFuture<Node> future = deleteAsync(path, version);
        future.get();
    }

    public CompletableFuture<GetDataResult> getDataAsync(String path) throws Exception {
        CompletableFuture<GetDataResult> future = new CompletableFuture<GetDataResult>();
        GetData requestOp = new GetData(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public Node getData(String path) throws Exception {
        return getDataAsync(path).get().getNode().get();
    }

    public CompletableFuture<GetChildrenResult> getChildrenAsync(String path) throws Exception {
        CompletableFuture<GetChildrenResult> future = new CompletableFuture<GetChildrenResult>();
        GetChildren requestOp = new GetChildren(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public List<String> getChildren(String path) throws Exception {
        return getChildrenAsync(path).get().getChildren();
    }

    public CompletableFuture<GetDataResult> existsAsync(String path) throws Exception {
        CompletableFuture<GetDataResult> future = new CompletableFuture<GetDataResult>();
        NodeExists requestOp = new NodeExists(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public Node exists(String path) throws Exception {
       return existsAsync(path).get().getNode().orElse(null);
    }

    public CompletableFuture<Node> deleteAsync(String path, int version) throws Exception {
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Missing session id in FK client");
        }
        DeleteNode requestOp = new DeleteNode(sessionId, path, version);
        CompletableFuture<Node> future = new CompletableFuture<Node>();
        workQueue.addRequest(requestOp, future);
        return future;
    }

}