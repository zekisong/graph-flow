package com.tencent.graphflow.hash;

import com.tencent.graphflow.server.cluster.ClusterListener;
import com.tencent.tdf.proto.Node;

public class ConsistentHashStrategy implements HashStrategy, ClusterListener {

    @Override
    public int hash(Object target) {
        return 0;
    }

    public static void main(String[] args) {
        System.out.println(Node.newBuilder().setAddress("localhost").setPort(123).build().hashCode());
    }

    @Override
    public void nodeJoin() {

    }

    @Override
    public void nodeRemove() {

    }
}
