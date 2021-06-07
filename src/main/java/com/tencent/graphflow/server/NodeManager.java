package com.tencent.graphflow.server;

import com.tencent.graphflow.LifeCycle;
import com.tencent.graphflow.server.cluster.ClusterContextAware;
import com.tencent.graphflow.server.cluster.zk.ZKClusterContextAware;
import com.tencent.graphflow.server.rpc.RpcServer;
import com.tencent.graphflow.server.rpc.RpcService;
import com.tencent.tdf.proto.Node;

public class NodeManager implements LifeCycle {

    private ClusterContextAware clusterContextAware;
    private RpcServer server;
    private FlowEngine flowEngine;

    public NodeManager() {
        this.clusterContextAware = new ZKClusterContextAware();
        this.server = new RpcServer(clusterContextAware.getMySelf());
        this.server.addService(new RpcService(this));
        this.flowEngine = new FlowEngine();
    }

    public FlowEngine getFlowEngine() {
        return flowEngine;
    }

    public ClusterContextAware getClusterContextAware() {
        return clusterContextAware;
    }

    public Node getMyself() {
        return clusterContextAware.getMySelf();
    }

    @Override
    public void start() {
        clusterContextAware.start();
        server.start();
    }

    @Override
    public void stop() {
        clusterContextAware.stop();
        server.stop();
    }
}
