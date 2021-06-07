package com.tencent.graphflow.flow.runtime;

import com.tencent.graphflow.LifeCycle;
import com.tencent.graphflow.constant.FlowConstant;
import com.tencent.graphflow.flow.model.Flow;
import com.tencent.graphflow.flow.model.FlowContext;
import com.tencent.graphflow.mgmt.ThreadManagement;
import com.tencent.graphflow.server.rpc.RpcServiceClient;
import com.tencent.tdf.proto.Node;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowRuntime<S, E> implements LifeCycle, IO {

    private static final Logger LOG = LoggerFactory.getLogger(FlowRuntime.class);
    private static final int BATCH_SIZE = 1024;
    private FlowContext<S, E> context;
    private List<FlowHandler> handlers;
    private PriorityBlockingQueue<Record> inputs;
    private PriorityBlockingQueue<Record> outputs;
    private List<AtomicLong> receives;
    private List<AtomicLong> sends;
    private AtomicReference<ThreadState> network;

    public FlowRuntime(FlowContext context) {
        this.context = context;
        this.handlers = new ArrayList<>();
        this.inputs = new PriorityBlockingQueue<>();
        this.outputs = new PriorityBlockingQueue<>();
        this.receives = new ArrayList<>();
        this.context.getFlow().getOperators().forEach(op -> receives.add(new AtomicLong(0)));
        this.sends = new ArrayList<>();
        this.context.getFlow().getOperators().forEach(op -> sends.add(new AtomicLong(0)));
        this.network = new AtomicReference<>(ThreadState.IDLE);
    }

    public FlowHandler randomHandler() {
        int index = (int) (Math.random() * handlers.size());
        return handlers.get(index);
    }

    @Override
    public void start() {
        int parallel = context.get(FlowConstant.TASK_COUNT_PER_EXECUTOR, FlowConstant.DEFAULT_TASK_COUNT_PER_EXECUTOR);
        for (int i = 0; i < parallel; i++) {
            Flow flow = context.getFlow();
            FlowHandler handler = new FlowHandler(this, flow);
            handlers.add(handler);
        }
    }

    @Override
    public void stop() {
    }

    public boolean isEmpty() {
        return inputs.size() == 0;
    }

    @Override
    public Record get() {
        return inputs.poll();
    }

    @Override
    public void put(Record record) {
        outputs.put(record);
        flush();
    }

    public boolean isEnd(Record record) {
        return context.size() == record.getOpIndex();
    }

    public void input(Record record) {
        receives.get(record.getOpIndex()).incrementAndGet();
        inputs.put(record);
        randomHandler().activation();
    }

    public void flush() {
        if (network.compareAndSet(ThreadState.IDLE, ThreadState.RUNNING)) {
            ThreadManagement.NETWORK.execute(() -> {
                try {
                    List<Record> sendRecords = new ArrayList<>();
                    Record tmp;
                    while ((tmp = outputs.poll()) != null) {
                        sendRecords.add(tmp);
                        if (sendRecords.size() > BATCH_SIZE) {
                            flushInternal(sendRecords);
                            sendRecords = new ArrayList<>();
                        }
                    }
                    if (sendRecords.size() > 0) {
                        flushInternal(sendRecords);
                    }
                } finally {
                    network.compareAndSet(ThreadState.RUNNING, ThreadState.IDLE);
                    if (outputs.size() > 0) {
                        ThreadManagement.NETWORK.execute(this::flush);
                    }
                }
            });
        }
    }

    public void flushInternal(List<Record> records) {
        List<Node> nodes = context.getClusterContext().getNodesList();
        Map<Integer, Long> countMap = new HashMap<>();
        records.stream()
                .collect(Collectors.groupingBy(record -> {
                    countMap.compute(record.getOpIndex() - 1, (k, v) -> {
                        if (v == null) {
                            return 1l;
                        } else {
                            return v + 1;
                        }
                    });
                    return record.getHash();
                }))
                .forEach((hash, batch) -> {
                    Node target = nodes.get(hash % nodes.size());
                    RpcServiceClient.getClient(target).process(Batch.encode(batch));
                });
        countMap.keySet().stream().forEach(index -> sends.get(index).addAndGet(countMap.get(index)));
    }
}
