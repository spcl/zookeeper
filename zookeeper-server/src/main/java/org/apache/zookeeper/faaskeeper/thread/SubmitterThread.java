package org.apache.zookeeper.faaskeeper.thread;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.zookeeper.faaskeeper.queue.WorkQueue;
import org.apache.zookeeper.faaskeeper.queue.EventQueue;
import org.apache.zookeeper.faaskeeper.queue.WorkQueueItem;
import org.apache.zookeeper.faaskeeper.provider.ProviderClient;
import org.apache.zookeeper.faaskeeper.provider.NodeDoesNotExist;
import org.apache.zookeeper.faaskeeper.model.DirectOperation;
import org.apache.zookeeper.faaskeeper.model.GetChildren;
import org.apache.zookeeper.faaskeeper.model.RegisterSession;
import org.apache.zookeeper.faaskeeper.model.GetData;
import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.model.NodeExists;
import org.apache.zookeeper.faaskeeper.model.ReadOpResult;
import org.apache.zookeeper.faaskeeper.model.RequestOperation;
import org.apache.zookeeper.faaskeeper.model.GetDataResult;
import org.apache.zookeeper.faaskeeper.model.GetChildrenResult;
import org.apache.zookeeper.faaskeeper.model.RegisterSessionResult;
import org.apache.zookeeper.faaskeeper.model.ReadExceptionResult;

public class SubmitterThread implements Runnable {
    private Future<?> future;
    private final ExecutorService executorService;
    private volatile boolean running = true;
    private static final Logger LOG;
    static {
        LOG = LoggerFactory.getLogger(SubmitterThread.class);
    }
    private final WorkQueue workQueue;
    private final EventQueue eventQueue;
    private final ProviderClient providerClient;
    private final String sessionID;

    public SubmitterThread(WorkQueue workQueue, EventQueue eventQueue, ProviderClient providerClient, String sessionID) {
        executorService = Executors.newSingleThreadExecutor();
        this.workQueue = workQueue;
        this.eventQueue = eventQueue;
        this.providerClient = providerClient;
        this.sessionID = sessionID;
        this.start();
    }

    public void start() {
        future = executorService.submit(this);
    }

    @Override
    public void run() {
        while(running) {

            Optional<WorkQueueItem> result = workQueue.get();
            if (!result.isPresent()) {
                // TODO: Remove this LOG later
                LOG.debug("work queue empty");
                continue;
            }

            WorkQueueItem request = result.get();

            try {

                if (request.operation instanceof RequestOperation) {
                    LOG.debug("Adding expected result to eventQueue");
                    RequestOperation op = (RequestOperation) request.operation;
                    eventQueue.addExpectedResult(request.requestID, op, request.future);

                    LOG.debug("Sending create req to providerClient");
                    providerClient.sendRequest(sessionID + "-" + String.valueOf(request.requestID), op.generateRequest());

                } else if (request.operation instanceof DirectOperation) {
                    String opName = request.operation.getName();

                    switch (opName) {
                        case "register_session":
                            RegisterSession reg_op = (RegisterSession) request.operation;
                            providerClient.registerSession(reg_op.getSessionId(), reg_op.sourceAddr, reg_op.heartbeat);
                            eventQueue.addDirectResult(request.requestID, new RegisterSessionResult(reg_op.getSessionId()), request.future);
                            break;

                        case "get_data":
                            GetData get_op = (GetData) request.operation;
                            Node n = providerClient.getData(get_op.getPath());
                            eventQueue.addDirectResult(request.requestID, new GetDataResult(n), request.future);
                            break;

                        case "exists":
                            try {
                                NodeExists exists_op = (NodeExists) request.operation;
                                n = providerClient.getData(exists_op.getPath());
                                eventQueue.addDirectResult(request.requestID, new GetDataResult(n), request.future);
                            } catch (NodeDoesNotExist ex) {
                                LOG.debug("Node does not exist");
                                eventQueue.addDirectResult(request.requestID, new GetDataResult(null), request.future);
                            }
                            break;

                        case "get_children":
                            GetChildren get_ch_op = (GetChildren) request.operation;
                            n = providerClient.getData(get_ch_op.getPath());
                            eventQueue.addDirectResult(request.requestID, new GetChildrenResult(n.getChildren()), request.future);
                            break;
                        default:
                            LOG.error("Unknown op type: " + opName);
                            break;
                    }
                } else {
                    LOG.error("Unknown request type: " + request.operation.getClass().getName());
                }

            } catch (Exception e) {
                LOG.debug("Exception in processing WorkQueue events in submitter thread", e);
                try {
                    eventQueue.addDirectResult(request.requestID, new ReadExceptionResult(e), request.future);
                } catch (Exception ex) {
                    LOG.error("Fatal error in SubmitterThread. Failed in adding DirectResult to eventQueue: ", ex);
                }
            }
        }

        LOG.debug("SubmitterThread loop exited successfully");
    }

    public void stop() {
        running = false;
        try {
            future.get();
            executorService.shutdown();
            LOG.debug("Successfully stopped Submitter thread");
        } catch (InterruptedException e) {
            LOG.error("Submitter Thread shutdown interrupted: ", e);
        } catch(ExecutionException e) {
            LOG.error("Error in Submitter thread execution: ", e);
        }
    }
}
