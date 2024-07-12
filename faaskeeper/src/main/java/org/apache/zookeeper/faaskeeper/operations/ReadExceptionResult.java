package org.apache.zookeeper.faaskeeper.operations;

public class ReadExceptionResult extends ReadOpResult {
    private Exception ex;
    public ReadExceptionResult(Exception ex) {
        this.ex = ex;
    }
    public Exception getException() {
        return ex;
    }
}