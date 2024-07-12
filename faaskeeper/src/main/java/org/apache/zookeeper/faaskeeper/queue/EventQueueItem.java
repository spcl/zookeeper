package org.apache.zookeeper.faaskeeper.queue;

public abstract class EventQueueItem {
    private long timestamp;

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }
}

enum EventType {
    CLOUD_INDIRECT_RESULT("CLOUD_INDIRECT_RESULT"),
    CLOUD_DIRECT_RESULT("CLOUD_DIRECT_RESULT"),
    CLOUD_EXPECTED_RESULT("CLOUD_EXPECTED_RESULT"),
    WATCH_NOTIFICATION("WATCH_NOTIFICATION");

    private final String value;

    EventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}