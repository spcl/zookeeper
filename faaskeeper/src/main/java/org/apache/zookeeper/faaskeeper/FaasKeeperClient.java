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
import org.apache.zookeeper.server.EphemeralType;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.faaskeeper.provider.ProviderClient;
import org.apache.zookeeper.faaskeeper.provider.AwsClient;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.operations.CreateNode;
import org.apache.zookeeper.faaskeeper.operations.DeleteNode;
import org.apache.zookeeper.faaskeeper.operations.GetChildren;
import org.apache.zookeeper.faaskeeper.operations.GetChildrenResult;
import org.apache.zookeeper.faaskeeper.operations.GetData;
import org.apache.zookeeper.faaskeeper.operations.GetDataResult;
import org.apache.zookeeper.faaskeeper.operations.NodeExists;
import org.apache.zookeeper.faaskeeper.operations.ReadOpResult;
import org.apache.zookeeper.faaskeeper.operations.RegisterSession;
import org.apache.zookeeper.faaskeeper.operations.SetData;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.client.Chroot;
import org.apache.zookeeper.common.PathUtils;


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
    private final Chroot chroot;
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
            this.chroot = Chroot.ofNullable(cfg.getChrootPath());
        } catch (Exception e) {
            LOG.error("Error in initializing provider client", e);
            throw new RuntimeException("Error in initializing provider client", e);
        }
    }

    private String prependChroot(String clientPath) {
        return chroot.prepend(clientPath);
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

    // TODO for create API: Implement all types of Znode: Sequential, Ephemeral, Container etc
    // TODO: Throw unimplemented exceptions for Znode types not implemented yet / Non null ACL passed
    public String create(
        final String path,
        byte[] data,
        List<ACL> acl,
        CreateMode createMode) throws Exception {
            if (!createMode.equals(CreateMode.PERSISTENT)) {
                throw new UnsupportedOperationException("This method is unimplemented");
            }

            final String clientPath = path;
            PathUtils.validatePath(clientPath, createMode.isSequential());
            EphemeralType.validateTTL(createMode, -1);

            final String serverPath = prependChroot(clientPath);

            return chroot.strip(createSync(serverPath, data, createMode.toFlag()).getPath());
    }

    public String create(
        final String path,
        byte[] data,
        List<ACL> acl,
        CreateMode createMode,
        Stat stat) throws Exception {
        return create(path, data, acl, createMode, stat, -1);
    }

    private void updateStat(Stat stat, Node n, CreateMode createMode) throws Exception {
        // TODO: Update stat value (cxzid, mzxid) once FK ID length reduction is implemented

        if (createMode.isEphemeral()) {
        // TODO: set ephemeral owner to sessionID but sessionID is a String
        // TODO: change sessionID to integer. Will have to implement this to support Ephemeral Znodes
        } else {
            stat.setEphemeralOwner(0);
        }

        if (n.hasChildren()) {
            stat.setNumChildren(n.getChildren().size());
        } else {
            stat.setNumChildren(0);
        }

        if (n.hasData()) {
            stat.setDataLength(n.getData().length);
        } else {
            stat.setDataLength(0);
        }

    }

    public String create(
        final String path,
        byte[] data,
        List<ACL> acl,
        CreateMode createMode,
        Stat stat,
        long ttl) throws Exception {
            if (ttl != -1 || !createMode.equals(CreateMode.PERSISTENT)) {
                throw new UnsupportedOperationException("This method is unimplemented");
            }

            final String clientPath = path;
            PathUtils.validatePath(clientPath, createMode.isSequential());
            EphemeralType.validateTTL(createMode, ttl);
    
            final String serverPath = prependChroot(clientPath);

            Node n = createSync(serverPath, data, createMode.toFlag());

            updateStat(stat, n, createMode);

            return chroot.strip(n.getPath());
    }

    // The asynchronous version of create
    public void create(
        final String path,
        byte[] data,
        List<ACL> acl,
        CreateMode createMode,
        StringCallback cb,
        Object ctx) throws Exception {
            if (!createMode.equals(CreateMode.PERSISTENT)) {
                throw new UnsupportedOperationException("This method is unimplemented");
            }

            final String clientPath = path;
            PathUtils.validatePath(clientPath, createMode.isSequential());
            EphemeralType.validateTTL(createMode, -1);

            final String serverPath = prependChroot(clientPath);
            cb = chroot.interceptCallback(cb);

            CreateNode requestOp = new CreateNode(sessionId, serverPath, data, createMode.toFlag());
            requestOp.setCallback(cb, ctx);

            workQueue.addRequest(requestOp, new CompletableFuture<Node>());
    }

    // The asynchronous version of create
    public void create(
    final String path,
    byte[] data,
    List<ACL> acl,
    CreateMode createMode,
    Create2Callback cb,
    Object ctx) throws Exception {
        create(path, data, acl, createMode, cb, ctx, -1);
    }

    // The asynchronous version of create
    public void create(
        final String path,
        byte[] data,
        List<ACL> acl,
        CreateMode createMode,
        Create2Callback cb,
        Object ctx,
        long ttl) throws Exception {
            if (ttl != -1 || !createMode.equals(CreateMode.PERSISTENT)) {
                throw new UnsupportedOperationException("This method is unimplemented");
            }

            final String clientPath = path;
            PathUtils.validatePath(clientPath, createMode.isSequential());
            EphemeralType.validateTTL(createMode, -1);

            final String serverPath = prependChroot(clientPath);
            cb = chroot.interceptCallback(cb);

            CreateNode requestOp = new CreateNode(sessionId, serverPath, data, createMode.toFlag());
            requestOp.setCallback(cb, ctx);

            workQueue.addRequest(requestOp, new CompletableFuture<Node>());
    }

    // TODO: Make createSync and createAsync private funcs once ZK compatible functions are implemented
    // flags represents createmode in its bit representation
    public Node createSync(String path, byte[] value, int flags) throws Exception {
        CompletableFuture<Node> future = createAsync(path, value, flags);
        Node n = future.get();
        return n;
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

    public Node setDataSync(String path, byte[] value, int version) throws Exception {
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

    public void deleteSync(String path, int version) throws Exception {
        CompletableFuture<Node> future = deleteAsync(path, version);
        future.get();
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

    public Node getDataSync(String path) throws Exception {
        return getDataAsync(path).get().getNode().get();
    }

    public CompletableFuture<GetDataResult> getDataAsync(String path) throws Exception {
        CompletableFuture<GetDataResult> future = new CompletableFuture<GetDataResult>();
        GetData requestOp = new GetData(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public List<String> getChildrenSync(String path) throws Exception {
        return getChildrenAsync(path).get().getChildren();
    }

    public CompletableFuture<GetChildrenResult> getChildrenAsync(String path) throws Exception {
        CompletableFuture<GetChildrenResult> future = new CompletableFuture<GetChildrenResult>();
        GetChildren requestOp = new GetChildren(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public Node existsSync(String path) throws Exception {
        return existsAsync(path).get().getNode().orElse(null);
    }

    public CompletableFuture<GetDataResult> existsAsync(String path) throws Exception {
        CompletableFuture<GetDataResult> future = new CompletableFuture<GetDataResult>();
        NodeExists requestOp = new NodeExists(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }
}