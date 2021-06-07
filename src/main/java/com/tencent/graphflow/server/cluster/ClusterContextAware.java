package com.tencent.graphflow.server.cluster;

import com.tencent.graphflow.LifeCycle;
import com.tencent.tdf.proto.ClusterContext;
import com.tencent.tdf.proto.Node;

public interface ClusterContextAware extends LifeCycle {

    Node getMySelf();

    ClusterContext snapshot();
}
