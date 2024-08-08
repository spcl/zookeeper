package org.apache.zookeeper.faaskeeper.queue;

public class CloudProviderException extends RuntimeException {
    public CloudProviderException() {
        super();
    }

    public CloudProviderException(String message) {
        super(message);
    }

    public CloudProviderException(Throwable cause) {
        super(cause);
    }
}
