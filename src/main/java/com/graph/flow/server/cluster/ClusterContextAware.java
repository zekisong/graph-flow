package com.graph.flow.server.cluster;

import com.graph.flow.LifeCycle;
import com.graph.flow.proto.ClusterContext;
import com.graph.flow.proto.Node;

public interface ClusterContextAware extends LifeCycle {

    Node getMySelf();

    ClusterContext snapshot();
}
