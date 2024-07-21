package org.apache.zookeeper.faaskeeper.operations;
import org.apache.zookeeper.Watcher;

public abstract class DirectOperation extends Operation {
    private Watcher watcher;
    
    public DirectOperation(String sessionId, String path, Watcher watch) {
        super(sessionId, path);
        this.watcher = watch;

    }

    public boolean isCloudRequest() {
        return false;
    }

    public Watcher getWatch() {
        return watcher;
    }
}
