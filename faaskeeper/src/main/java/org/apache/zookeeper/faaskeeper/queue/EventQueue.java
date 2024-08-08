package org.apache.zookeeper.faaskeeper.queue;

import java.util.Optional; 
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.zookeeper.faaskeeper.model.Node;
import org.apache.zookeeper.faaskeeper.operations.DirectOperation;
import org.apache.zookeeper.faaskeeper.operations.ReadOpResult;
import org.apache.zookeeper.faaskeeper.operations.RequestOperation;

public class EventQueue {
    private static final Logger LOG;
    private LinkedBlockingQueue<EventQueueItem> queue;
    static {
        LOG = LoggerFactory.getLogger(EventQueue.class);
    }
    // private Map<String, List<Watch>> _watches;
    // private Lock _watchesLock;
    private boolean closing;

    public EventQueue() {
        // Initialize queue, watches, lock, and logger
        queue = new LinkedBlockingQueue<EventQueueItem>();
        closing = false;
    }

    public Optional<EventQueueItem> get() {
        try {
            return Optional.ofNullable(queue.poll(500, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            LOG.debug("Queue poll interrupted", e);
            return Optional.empty();
        }
    }

    public void close() {
        closing = true;
    }

    // public void addExpectedResult(int requestId, Operation request, Future future) {
    // }

    public void addDirectResult(int requestId, ReadOpResult result, CompletableFuture<ReadOpResult> future, DirectOperation op) throws RuntimeException {
        if (closing) {
            throw new RuntimeException("Cannot add result to queue: EventQueue has been closed");
        }

        try {
            queue.add(new CloudDirectResult(requestId, result, future, op));
        } catch (IllegalStateException e) {
            LOG.error("EventQueue add item failed", e);
            throw e;
        }
    }

    public void addIndirectResult(JsonNode result) throws RuntimeException {
        if (closing) {
            throw new RuntimeException("Cannot add result to queue: EventQueue has been closed");
        }
        try {
            queue.add(new CloudJsonResult(result));
        } catch (IllegalStateException e) {
            LOG.error("EventQueue add item failed", e);
            throw e;
        }
    }

    public void addExpectedResult(int requestID, RequestOperation op, CompletableFuture<Node> future) throws RuntimeException {
        if (closing) {
            throw new RuntimeException("Cannot add result to queue: EventQueue has been closed");
        }
        try {
            queue.add(new CloudExpectedResult(requestID, op, future));
        } catch (IllegalStateException e) {
            LOG.error("EventQueue add item failed", e);
            throw e;
        }
    }

    public void addProviderError(RequestOperation op, CloudProviderException ex, CompletableFuture<Node> future) throws RuntimeException {
        if (closing) {
            throw new RuntimeException("Cannot add result to queue: EventQueue has been closed");
        }
        try {
            queue.add(new CloudErrorResult(op, ex, future));
        } catch (IllegalStateException e) {
            LOG.error("EventQueue add item failed", e);
            throw e;
        }
    }
}