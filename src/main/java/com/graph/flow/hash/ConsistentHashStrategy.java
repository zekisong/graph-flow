package com.graph.flow.hash;

import com.graph.flow.server.cluster.ClusterListener;

public class ConsistentHashStrategy implements HashStrategy, ClusterListener {

    @Override
    public int hash(Object target) {
        return 0;
    }

    @Override
    public void nodeJoin() {

    }

    @Override
    public void nodeRemove() {

    }
}
