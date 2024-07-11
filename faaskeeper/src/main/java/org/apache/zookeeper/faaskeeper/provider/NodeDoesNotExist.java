package org.apache.zookeeper.faaskeeper.provider;

public class NodeDoesNotExist extends Exception {
    public NodeDoesNotExist(String message) {
        super(message);
    }
}
