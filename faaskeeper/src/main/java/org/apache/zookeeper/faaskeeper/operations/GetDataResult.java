package org.apache.zookeeper.faaskeeper.operations;
import java.util.Optional;

import org.apache.zookeeper.faaskeeper.model.Node;

public class GetDataResult extends ReadOpResult {
    private Optional<Node> node;

    public GetDataResult(Node node) {
        this.node = Optional.ofNullable(node);
    }

    public Optional<Node> getNode() {
        return node;
    }
}
