package com.tencent.graphflow.server;

import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.tencent.graphflow.exception.SerializeException;
import com.tencent.graphflow.flow.model.FlowContext;
import com.tencent.graphflow.flow.runtime.Batch;
import com.tencent.graphflow.flow.runtime.FlowRuntime;
import com.tencent.graphflow.flow.runtime.FlowScheduler;
import com.tencent.graphflow.flow.runtime.Record;
import com.tencent.graphflow.serde.JavaSerDe;
import com.tencent.graphflow.serde.SerDe;
import com.tencent.graphflow.server.rpc.RpcServiceClient;
import com.tencent.tdf.proto.Node;
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

    public FlowEngine() {
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
        Record start = new Record(context.getFlowId(), "start");
        RpcServiceClient.getClient(context.getCoordinator()).process(Batch.encode(Lists.newArrayList(start)));
        FlowScheduler scheduler = new FlowScheduler(this, context);
        this.submit.put(context.getFlowId(), scheduler);
        return scheduler;
    }

    public void destroy(Long flowId) {
        FlowRuntime runtime = running.remove(flowId);
        runtime.stop();
        submit.remove(flowId);
    }

    public <T extends Record> void process(List<T> record) {
        record.forEach(r -> {
            Long flowId = r.getFlowId();
            FlowRuntime runtime = running.get(flowId);
            if (runtime.isEnd(r)) {
                FlowScheduler scheduler = submit.get(flowId);
                scheduler.addResult(r.getRecord());
            } else {
                runtime.input(r);
            }
        });
    }

    public void bootstrap(ByteString data) throws SerializeException {
        FlowContext context = serDe.deSerialize(data.toByteArray());
        LOG.info("bootstrap context:" + context.getFlowId());
        FlowRuntime runtime = new FlowRuntime(context);
        running.put(context.getFlowId(), runtime);
        runtime.start();
    }
}
