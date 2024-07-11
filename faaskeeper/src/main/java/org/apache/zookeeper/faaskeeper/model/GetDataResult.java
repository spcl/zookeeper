package org.apache.zookeeper.faaskeeper.model;
import java.util.Optional;

public class GetDataResult extends ReadOpResult {
    private Optional<Node> node;

    public GetDataResult(Node node) {
        this.node = Optional.ofNullable(node);
    }

    public Optional<Node> getNode() {
        return node;
    }
}
