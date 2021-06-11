package com.graph.flow.server;

import com.graph.flow.LifeCycle;
import com.graph.flow.proto.Node;
import com.graph.flow.server.cluster.ClusterContextAware;
import com.graph.flow.server.cluster.zk.ZKClusterContextAware;
import com.graph.flow.server.rpc.RpcServer;
import com.graph.flow.server.rpc.RpcService;

public class NodeManager implements LifeCycle {

    private ClusterContextAware clusterContextAware;
    private RpcServer server;
    private FlowEngine flowEngine;

    public NodeManager() {
        this.clusterContextAware = new ZKClusterContextAware();
        this.server = new RpcServer(clusterContextAware.getMySelf());
        this.server.addService(new RpcService(this));
        this.flowEngine = new FlowEngine(clusterContextAware.getMySelf());
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
