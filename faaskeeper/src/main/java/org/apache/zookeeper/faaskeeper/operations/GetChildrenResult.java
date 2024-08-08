package org.apache.zookeeper.faaskeeper.operations;

import java.util.List;
import org.apache.zookeeper.data.Stat;

public class GetChildrenResult extends ReadOpResult {
    private List<String> children;
    private Stat stat;

    public GetChildrenResult(List<String> children, Stat stat) {
        this.children = children;
        this.stat = stat;
    }

    public List<String> getChildren() {
        if (children == null) {
            throw new NullPointerException("Children list is null");
        }
        return children;
    }

    public Stat getStat() {
        if (stat == null) {
            throw new NullPointerException("Stat is null");
        }
        return stat;
    }
}