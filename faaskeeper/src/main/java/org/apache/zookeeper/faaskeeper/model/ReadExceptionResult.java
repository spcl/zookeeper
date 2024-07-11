package org.apache.zookeeper.faaskeeper.model;

public class ReadExceptionResult extends ReadOpResult {
    private Exception ex;
    public ReadExceptionResult(Exception ex) {
        this.ex = ex;
    }
    public Exception getException() {
        return ex;
    }
}