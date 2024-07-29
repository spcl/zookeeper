package org.apache.zookeeper.faaskeeper.queue;

import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.operations.RequestOperation;

import java.util.concurrent.CompletableFuture;

public class CloudErrorResult extends CloudIndirectResult {
    public final CloudProviderException cloudException;
    public final CompletableFuture<Node> future;
    public final RequestOperation op;

    public CloudErrorResult(RequestOperation op, CloudProviderException ex, CompletableFuture<Node> future) {
        super();
        cloudException = ex;
        this.future = future;
        this.op = op;
    }
}