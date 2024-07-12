package org.apache.zookeeper.faaskeeper.operations;

import java.util.List;

public class GetChildrenResult extends ReadOpResult {
    private List<String> children;

    public GetChildrenResult(List<String> children) {
        this.children = children;
    }

    public List<String> getChildren() {
        if (children == null) {
            throw new NullPointerException("Children list is null");
        }
        return children;
    }
}