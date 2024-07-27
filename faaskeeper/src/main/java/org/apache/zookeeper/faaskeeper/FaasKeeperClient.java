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
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.EphemeralType;

import java.util.concurrent.CancellationException;
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
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.ChildrenCallback;
import org.apache.zookeeper.AsyncCallback.Create2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
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
    private Watcher defaultWatcher;
    static {
        LOG = LoggerFactory.getLogger(FaasKeeperClient.class);
        providers.put(CloudProvider.serialize(CloudProvider.AWS), AwsClient.class);
    }

    public FaasKeeperClient(FaasKeeperConfig cfg, boolean heartbeat, Watcher watcher) {
        try {
            this.cfg = cfg;
            this.heartbeat = heartbeat;
            Class<? extends ProviderClient> providerClass = providers.get(CloudProvider.serialize(this.cfg.getCloudProvider()));
            this.providerClient = providerClass.getDeclaredConstructor(FaasKeeperConfig.class).newInstance(this.cfg);
            this.eventQueue = new EventQueue();
            this.workQueue = new WorkQueue();
            this.chroot = Chroot.ofNullable(cfg.getChrootPath());
            this.defaultWatcher = watcher;
        } catch (Exception e) {
            LOG.error("Error in initializing provider client", e);
            throw new RuntimeException("Error in initializing provider client", e);
        }
    }

    private Watcher getDefaultWatcher(boolean watch) {
        if (watch) {
            if (this.defaultWatcher == null) {
                // TODO: Once watcher is implemented, throw IllegalStateException if watch is True but defaultWatcher is null. (Similar to the ZK getDefaultWatcher method)
            }
            return this.defaultWatcher;
        }

        return null;
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

    public static FaasKeeperClient buildClient(String configFilePath, boolean heartbeat, Watcher watcher) throws Exception {
        try {
            FaasKeeperConfig cfg = FaasKeeperConfig.buildFromConfigJson(configFilePath);
            return new FaasKeeperClient(cfg, heartbeat, watcher);
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
        CreateMode createMode) throws KeeperException, InterruptedException {
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
        Stat stat) throws KeeperException, InterruptedException {
        return create(path, data, acl, createMode, stat, -1);
    }

    public static void updateStat(Stat stat, Node n, CreateMode createMode) {
        // TODO: currently createMode null is accepted because NodeType is unknown in DDB

        if (n == null) {
            throw new RuntimeException("Node cannot be null");
        }
        if (stat == null) {
            throw new RuntimeException("Stat cannot be null");
        }
        // TODO: Update stat value (cxzid, mzxid) once FK ID length reduction is implemented
        // TODO: Node has to store it's CreateMode (persistent of ephemeral)
        if (createMode != null && createMode.isEphemeral()) {
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
    // TODO: Add session ID check to every ZK API
    public String create(
        final String path,
        byte[] data,
        List<ACL> acl,
        CreateMode createMode,
        Stat stat,
        long ttl) throws KeeperException, InterruptedException {
            if (ttl != -1 || !createMode.equals(CreateMode.PERSISTENT)) {
                throw new UnsupportedOperationException("This method is unimplemented");
            }

            final String clientPath = path;
            PathUtils.validatePath(clientPath, createMode.isSequential());
            EphemeralType.validateTTL(createMode, ttl);
    
            final String serverPath = prependChroot(clientPath);

            Node n = createSync(serverPath, data, createMode.toFlag());

            if (stat != null) {
                updateStat(stat, n, createMode);
            }

            return chroot.strip(n.getPath());
    }

    // The asynchronous version of create
    public void create(
        final String path,
        byte[] data,
        List<ACL> acl,
        CreateMode createMode,
        StringCallback cb,
        Object ctx) {
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
    Object ctx) {
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
        long ttl) {
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
    public Node createSync(String path, byte[] value, int flags) throws KeeperException, InterruptedException {
        CompletableFuture<Node> future = createAsync(path, value, flags);

        try {
            Node n = future.get();
            return n;
        } catch (ExecutionException | CancellationException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Node> createAsync(String path, byte[] value, int flags) {
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Missing session id in FK client");
        }

        if (value == null) {
            value = new byte[]{};
        }

        CreateNode requestOp = new CreateNode(sessionId, path, value, flags);
        CompletableFuture<Node> future = new CompletableFuture<Node>();
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public void delete(final String path, int version) throws InterruptedException, KeeperException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath;

        // maintain semantics even in chroot case
        // specifically - root cannot be deleted
        // I think this makes sense even in chroot case.
        if (clientPath.equals("/")) {
            // a bit of a hack, but delete(/) will never succeed and ensures
            // that the same semantics are maintained
            serverPath = clientPath;
        } else {
            serverPath = prependChroot(clientPath);
        }

        deleteSync(serverPath, version);
    }

    /*
     * The asynchronous version of delete.
    */
    public void delete(final String path, int version, VoidCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        final String serverPath;

        // maintain semantics even in chroot case
        // specifically - root cannot be deleted
        // I think this makes sense even in chroot case.
        if (clientPath.equals("/")) {
            // a bit of a hack, but delete(/) will never succeed and ensures
            // that the same semantics are maintained
            serverPath = clientPath;
        } else {
            serverPath = prependChroot(clientPath);
        }

        DeleteNode requestOp = new DeleteNode(sessionId, serverPath, version);
        requestOp.setCallback(cb, ctx);
        
        workQueue.addRequest(requestOp, new CompletableFuture<Node>());
    }


    public Node setDataSync(String path, byte[] value, int version) throws KeeperException, InterruptedException {
        CompletableFuture<Node> future = setDataAsync(path, value, version);
        try {
            return future.get();
        } catch (ExecutionException | CancellationException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Node> setDataAsync(String path, byte[] value, int version) {
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Missing session id in FK client");
        }

        if (value == null) {
            value = new byte[]{};
        }

        SetData requestOp = new SetData(sessionId, path, value, version);
        CompletableFuture<Node> future = new CompletableFuture<Node>();
        workQueue.addRequest(requestOp, future);

        return future;
    }

    public void deleteSync(String path, int version) throws InterruptedException, KeeperException {
        CompletableFuture<Node> future = deleteAsync(path, version);
        
        try {
            future.get();
        } catch (ExecutionException | CancellationException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<Node> deleteAsync(String path, int version) throws KeeperException {
        if (sessionId == null || sessionId.isEmpty()) {
            throw new RuntimeException("Missing session id in FK client");
        }
        DeleteNode requestOp = new DeleteNode(sessionId, path, version);
        CompletableFuture<Node> future = new CompletableFuture<Node>();
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public Node getDataSync(String path) throws KeeperException, InterruptedException {
        try {
            return getDataAsync(path).get().getNode().get();
        } catch (ExecutionException | CancellationException e) {
            throw new RuntimeException(e);
        } 
    }

    public CompletableFuture<GetDataResult> getDataAsync(String path) {
        CompletableFuture<GetDataResult> future = new CompletableFuture<GetDataResult>();
        GetData requestOp = new GetData(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public GetChildrenResult getChildrenSync(String path) throws KeeperException, InterruptedException {
        try {
            return getChildrenAsync(path).get();
        } catch (ExecutionException | CancellationException e) {
            throw new RuntimeException(e);
        }
    }

    public CompletableFuture<GetChildrenResult> getChildrenAsync(String path) {
        CompletableFuture<GetChildrenResult> future = new CompletableFuture<GetChildrenResult>();
        GetChildren requestOp = new GetChildren(sessionId, path, null);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    public Node existsSync(String path, Watcher watcher) throws  KeeperException, InterruptedException {
        try {
            return existsAsync(path, watcher).get().getNode().orElse(null);
        } catch (ExecutionException | CancellationException e) {
                throw new RuntimeException(e);
        }
    }

    public CompletableFuture<GetDataResult> existsAsync(String path, Watcher watcher) throws KeeperException {
        CompletableFuture<GetDataResult> future = new CompletableFuture<GetDataResult>();
        NodeExists requestOp = new NodeExists(sessionId, path, watcher);
        workQueue.addRequest(requestOp, future);
        return future;
    }

    // TODO: Support watches for exists
    public Stat exists(final String path, Watcher watcher) throws  KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new ExistsWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);
        Stat stat = new Stat();
        Node n = existsSync(serverPath, watcher);
        if (n == null) {
            return null;
        }

        updateStat(stat, n, null);

        return stat;
    }

    public Stat exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return exists(path, getDefaultWatcher(watch));
    }

    /*
     * The asynchronous version of exists.
     */
    public void exists(final String path, Watcher watcher, StatCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new ExistsWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        CompletableFuture<GetDataResult> future = new CompletableFuture<GetDataResult>();
        NodeExists requestOp = new NodeExists(sessionId, serverPath, watcher);
        requestOp.setCallback(cb, ctx);
        workQueue.addRequest(requestOp, future);
   
    }

    /*
     * The asynchronous version of exists.
     */
    public void exists(String path, boolean watch, StatCallback cb, Object ctx) {
        exists(path,  getDefaultWatcher(watch), cb, ctx);
    }

    public byte[] getData(final String path, Watcher watcher, Stat stat)  throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new DataWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        Node n = getDataSync(serverPath);

        byte[] data = n.getData();

        if (stat != null) {
            // TODO: We do not know the createMode of each node yet. This is not stored in DDB yet
            updateStat(stat, n, null);
        }

        return data;
    }

    public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException, InterruptedException {
        return getData(path, getDefaultWatcher(watch), stat);
    }

        /**
     * The asynchronous version of getData.
     *
     * @see #getData(String, Watcher, Stat)
     */
    public void getData(final String path, Watcher watcher, DataCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new DataWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        GetData requestOp = new GetData(sessionId, serverPath, watcher);
        requestOp.setCallback(cb, ctx);

        workQueue.addRequest(requestOp, new CompletableFuture<GetDataResult>());
    }

    /*
     * The asynchronous version of getData.
     */
    public void getData(String path, boolean watch, DataCallback cb, Object ctx) {
        getData(path, getDefaultWatcher(watch), cb, ctx);
    }

    public Stat setData(final String path, byte[] data, int version) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        if (data.length > 1024 * 1024) {
            LOG.debug("Data length more than 1MB");
            throw new RuntimeException("Data length more than 1MB");
        }

        final String serverPath = prependChroot(clientPath);

        Node n = setDataSync(serverPath, data, version);

        Stat stat = new Stat();
        updateStat(stat, n, null);

        return stat;
    }

    /*
     * The asynchronous version of setData.
     */
    public void setData(final String path, byte[] data, int version, StatCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        if (data.length > 1024 * 1024) {
            LOG.debug("Data length more than 1MB");
            throw new RuntimeException("Data length more than 1MB");
        }

        final String serverPath = prependChroot(clientPath);

        SetData requestOp = new SetData(sessionId, serverPath, data, version);
        requestOp.setCallback(cb, ctx);

        workQueue.addRequest(requestOp, new CompletableFuture<Node>());
    }

    public List<String> getChildren(final String path, Watcher watcher) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        return getChildrenSync(serverPath).getChildren();   
    }

    public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
        return getChildren(path, getDefaultWatcher(watch));
    }

    /*
     * The asynchronous version of getChildren.
     */
    public void getChildren(final String path, Watcher watcher, ChildrenCallback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        GetChildren requestOp = new GetChildren(sessionId, serverPath, watcher);
        requestOp.setCallback(cb, ctx);
        workQueue.addRequest(requestOp, new CompletableFuture<GetChildrenResult>());
    }

    /*
     * The asynchronous version of getChildren.
     */
    public void getChildren(String path, boolean watch, ChildrenCallback cb, Object ctx) {
        getChildren(path, getDefaultWatcher(watch), cb, ctx);
    }

    public List<String> getChildren(
        final String path,
        Watcher watcher,
        Stat stat) throws KeeperException, InterruptedException {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);
        GetChildrenResult res = getChildrenSync(serverPath);

        if (stat != null) {
            DataTree.copyStat(res.getStat(), stat);
        }

        return res.getChildren();
    }

    public List<String> getChildren(
        String path,
        boolean watch,
        Stat stat) throws KeeperException, InterruptedException {
        return getChildren(path, getDefaultWatcher(watch), stat);
    }

    /*
     * The asynchronous version of getChildren.
     */
    public void getChildren(final String path, Watcher watcher, Children2Callback cb, Object ctx) {
        final String clientPath = path;
        PathUtils.validatePath(clientPath);

        // the watch contains the un-chroot path
        // WatchRegistration wcb = null;
        if (watcher != null) {
            // wcb = new ChildWatchRegistration(watcher, clientPath);
        }

        final String serverPath = prependChroot(clientPath);

        GetChildren requestOp = new GetChildren(sessionId, serverPath, watcher);
        requestOp.setCallback(cb, ctx);
        workQueue.addRequest(requestOp, new CompletableFuture<GetChildrenResult>());
    }

    public void getChildren(String path, boolean watch, Children2Callback cb, Object ctx) {
        getChildren(path, getDefaultWatcher(watch), cb, ctx);
    }


}