package com.tencent.graphflow.flow.model;

import com.google.common.collect.Lists;
import com.tencent.graphflow.config.FlowConfig;
import com.tencent.graphflow.exception.NoNodeAvailableException;
import com.tencent.graphflow.exception.SerializeException;
import com.tencent.graphflow.flow.runtime.Batch;
import com.tencent.graphflow.flow.runtime.Record;
import com.tencent.graphflow.hash.HashStrategy;
import com.tencent.graphflow.server.NodeManager;
import com.tencent.graphflow.server.rpc.RpcServiceClient;
import com.tencent.tdf.proto.ClusterContext;
import com.tencent.tdf.proto.Node;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class FlowContext<S, E> implements Serializable {

    private Long flowId = System.currentTimeMillis();
    private ClusterContext clusterContext;
    private Flow flow;
    private Node coordinator;
    private Map<String, Object> conf;
    private transient List<Operator> operators;

    public FlowContext(Node coordinator, ClusterContext clusterContext) {
        this.coordinator = coordinator;
        this.clusterContext = clusterContext;
        operators = new ArrayList();
        conf = new HashMap<>();
        conf.putAll(FlowConfig.getInstance());
    }

    public <N> FlowContext(FlowContext<S, N> parent) {
        this.coordinator = parent.coordinator;
        this.clusterContext = parent.clusterContext;
        this.conf = parent.conf;
        this.operators = parent.operators;
        this.flowId = parent.flowId;
    }

    public ClusterContext getClusterContext() {
        return clusterContext;
    }

    public Long getFlowId() {
        return flowId;
    }

    public Flow getFlow() {
        return flow;
    }

    public int size() {
        return flow.getOperators().size();
    }

    public <N> FlowContext<S, N> flatMap(Computer<E, Iterator<N>> computer) {
        operators.add(new Operator(computer, operators.size()));
        return new FlowContext<>(this);
    }

    public <N> FlowContext<S, N> flatMap(Computer<E, Iterator<N>> computer, HashStrategy<N> strategy) {
        operators.add(new Operator(computer, strategy));
        return new FlowContext<>(this);
    }

    public FlowContext<S, E> complete() {
        //result sink to current node
        int index = clusterContext.getNodesList().indexOf(coordinator);
        operators.add(new Operator(i -> Lists.newArrayList(i).iterator(), o -> index));
        this.flow = new Flow(operators);
        List<Node> nodes = clusterContext.getNodesList();
        if (nodes.size() == 0) {
            throw new NoNodeAvailableException("there is no node available to execute flow!");
        }
        return this;
    }

    public <T> FlowContext<S, E> conf(String key, T value) {
        conf.put(key, value);
        return this;
    }

    public <T> T get(String key, T defaultValue) {
        Object value = conf.get(key);
        return value == null ? defaultValue : (T) value;
    }

    public List<Operator> getOperators() {
        return operators;
    }

    public Node getCoordinator() {
        return coordinator;
    }

    public static <I, O> FlowContext<I, O> newContext(NodeManager nodeManager) {
        Node myself = nodeManager.getClusterContextAware().getMySelf();
        ClusterContext clusterContext = nodeManager.getClusterContextAware().snapshot();
        return new FlowContext(myself, clusterContext);
    }
}
