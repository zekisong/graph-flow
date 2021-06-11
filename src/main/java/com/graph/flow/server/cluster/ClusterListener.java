package com.graph.flow.server.cluster;

public interface ClusterListener {

    void nodeJoin();

    void nodeRemove();
}
