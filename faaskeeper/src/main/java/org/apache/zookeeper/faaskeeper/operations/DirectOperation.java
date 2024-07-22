package org.apache.zookeeper.faaskeeper.operations;
import org.apache.zookeeper.Watcher;

public abstract class DirectOperation extends Operation {
    private Watcher watcher;
    
    public DirectOperation(String sessionId, String path, Watcher watcher) {
        super(sessionId, path);
        this.watcher = watcher;
    }

    public boolean isCloudRequest() {
        return false;
    }

    public Watcher getWatch() {
        return watcher;
    }
}
