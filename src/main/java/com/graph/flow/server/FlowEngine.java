package com.graph.flow.server;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.graph.flow.exception.SerializeException;
import com.graph.flow.flow.model.FlowContext;
import com.graph.flow.flow.runtime.Batch;
import com.graph.flow.flow.runtime.FlowRuntime;
import com.graph.flow.flow.runtime.FlowScheduler;
import com.graph.flow.flow.runtime.Record;
import com.graph.flow.proto.Node;
import com.graph.flow.serde.JavaSerDe;
import com.graph.flow.serde.SerDe;
import com.graph.flow.server.rpc.RpcServiceClient;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowEngine {

    private static final Logger LOG = LoggerFactory.getLogger(FlowEngine.class);
    private Map<Long, FlowRuntime> running;
    private Map<Long, FlowScheduler> submit;
    private SerDe serDe;
    private Node myself;

    public FlowEngine(Node myself) {
        this.myself = myself;
        this.running = new HashMap<>();
        this.submit = new HashMap<>();
        this.serDe = new JavaSerDe();
    }

    public <S, E> Iterator<E> submit(FlowContext<S, E> context) throws SerializeException {
        List<Node> nodes = context.getClusterContext().getNodesList();
        for (Node node : nodes) {
            RpcServiceClient client = RpcServiceClient.getClient(node);
            client.bootstrap(context);
        }
        Record start = new Record("start");
        RpcServiceClient.getClient(context.getCoordinator())
                .process(context.getFlowId(), 0, new Batch(Lists.newArrayList(start)).encode());
        FlowScheduler scheduler = new FlowScheduler(this, context);
        this.submit.put(context.getFlowId(), scheduler);
        return scheduler;
    }

    public void destroy(Long flowId) {
        FlowRuntime runtime = running.remove(flowId);
        runtime.stop();
        submit.remove(flowId);
    }

    public <T extends Record> void process(long flowId, int index, Batch batch) {
        FlowRuntime runtime = running.get(flowId);
        if (runtime.isEnd(index)) {
            FlowScheduler scheduler = submit.get(flowId);
            batch.get().forEach(r -> scheduler.addResult(r));
        } else {
            runtime.input(index, batch);
        }
    }

    public void send(Node target, long flowId, int index, Batch batch) {
        if (target.equals(myself)) {
            process(flowId, index, batch);
        } else {
            RpcServiceClient.getClient(target).processAsync(flowId, index, batch.encode());
        }
    }

    public void bootstrap(ByteString data) throws SerializeException {
        FlowContext context = serDe.deSerialize(data.toByteArray());
        LOG.info("bootstrap context:" + context.getFlowId());
        FlowRuntime runtime = new FlowRuntime(this, context);
        running.put(context.getFlowId(), runtime);
        runtime.start();
    }
}
