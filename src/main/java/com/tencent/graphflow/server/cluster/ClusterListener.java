package com.tencent.graphflow.server.cluster;

public interface ClusterListener {

    void nodeJoin();

    void nodeRemove();
}
