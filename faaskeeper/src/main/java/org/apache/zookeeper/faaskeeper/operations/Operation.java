package org.apache.zookeeper.faaskeeper.operations;

import org.apache.zookeeper.AsyncCallback;

/**
 * Abstract base class for all provider-agnostic operations submitted to FK instance.
 */
public abstract class Operation {
    protected String path;
    protected String sessionId;
    protected Object callbackCtx;
    protected AsyncCallback cb;

    public Operation(String sessionId, String path) {
        this.sessionId = sessionId;
        this.path = path;
    }

    public void setCallback(AsyncCallback cb, Object callbackCtx) {
        this.cb = cb;
        this.callbackCtx = callbackCtx;
    }

    public String getSessionId() {
        return sessionId;
    }

    public String getPath() {
        return path;
    }

    public abstract String getName();
}
