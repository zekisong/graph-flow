package com.graph.flow.flow.runtime;

import com.graph.flow.LifeCycle;
import com.graph.flow.constant.FlowConstant;
import com.graph.flow.flow.model.Flow;
import com.graph.flow.flow.model.FlowContext;
import com.graph.flow.mgmt.ThreadManagement;
import com.graph.flow.proto.Node;
import com.graph.flow.server.FlowEngine;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowRuntime<S, E> implements LifeCycle, IO {

    private static final Logger LOG = LoggerFactory.getLogger(FlowRuntime.class);
    private static final int BATCH_SIZE = 10000;
    private FlowEngine engine;
    private FlowContext<S, E> context;
    private List<FlowHandler> handlers;
    private List<LinkedBlockingQueue<Batch>> inputs;
    private List<LinkedBlockingQueue<Batch>> outputs;
    private List<AtomicLong> receives;
    private List<AtomicLong> sends;
    private AtomicInteger flushCount = new AtomicInteger(0);

    public FlowRuntime(FlowEngine engine, FlowContext context) {
        this.engine = engine;
        this.context = context;
        this.handlers = new ArrayList<>();
        this.inputs = new ArrayList<>();
        this.outputs = new ArrayList<>();
        this.context.getFlow().getOperators().forEach(op -> inputs.add(new LinkedBlockingQueue<>(10240000)));
        this.context.getFlow().getOperators().forEach(op -> outputs.add(new LinkedBlockingQueue<>(10240000)));
        this.receives = new ArrayList<>();
        this.context.getFlow().getOperators().forEach(op -> receives.add(new AtomicLong(0)));
        this.sends = new ArrayList<>();
        this.context.getFlow().getOperators().forEach(op -> sends.add(new AtomicLong(0)));
        new ScheduledThreadPoolExecutor(1, r -> new Thread(r)).scheduleAtFixedRate(() -> {
            System.out.print("input:" + inputs.stream().map(q -> q.size()).collect(Collectors.toList()) + "\t");
            System.out.print("output:" + outputs.stream().map(q -> q.size()).collect(Collectors.toList()) + "\t");
            System.out.print("sends:" + sends.stream().map(l -> l.get()).collect(Collectors.toList()) + "\t");
            System.out.print("receives:" + receives.stream().map(l -> l.get()).collect(Collectors.toList()) + "\t");
            System.out.println();
        }, 100, 100, TimeUnit.MILLISECONDS);
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

    public boolean hasInput() {
        for (LinkedBlockingQueue queue : inputs) {
            if (queue.size() > 0) {
                return true;
            }
        }
        return false;
    }

    public boolean hasOutput() {
        for (LinkedBlockingQueue queue : outputs) {
            if (queue.size() > 0) {
                return true;
            }
        }
        return false;
    }

    public void input(int index, Batch batch) {
        receives.get(index).addAndGet(batch.size());
        inputs.get(index).add(batch);
        randomHandler().activation();
    }

    @Override
    public Batch poll(int opIndex) {
        return inputs.get(opIndex).poll();
    }

    @Override
    public Batch peek(int opIndex) {
        return inputs.get(opIndex).peek();
    }

    @Override
    public void put(int index, Batch batch) {
        Queue queue = outputs.get(index);
        queue.add(batch);
        tryFlush();
    }

    public void checkAndReport() {
        if (inputs.size() == 0 && outputs.size() == 0) {
        }
    }

    public boolean isEnd(int index) {
        return context.size() == index;
    }

    public void tryFlush() {
        while (true) {
            int count = flushCount.get();
            if (count < 1) {
                if (flushCount.compareAndSet(count, count + 1)) {
                    ThreadManagement.NETWORK.execute(() -> {
                        try {
                            flush();
                        } finally {
                            if (hasOutput() || hasInput()) {
                                ThreadManagement.NETWORK.execute(this::flush);
                            } else {
                                flushCount.decrementAndGet();
                            }
                        }
                    });
                    break;
                }
            } else {
                break;
            }
        }
    }

    public void flush() {
        while (true) {
            if (!hasInput() && !hasOutput()) {
                break;
            } else {
                int index = chooseMaxOutput();
                LinkedBlockingQueue<Batch> queue = outputs.get(index);
                List<Record> toFlush = new ArrayList<>();
                Batch batch;
                while ((batch = queue.poll()) != null) {
                    sends.get(index).addAndGet(batch.size());
                    toFlush.addAll(batch.get());
                    if (toFlush.size() < BATCH_SIZE) {
                        break;
                    }
                }
                flushInternal(index, toFlush);
            }
        }
    }

    public int chooseMaxOutput() {
        int index = 0;
        int maxSize = Integer.MIN_VALUE;
        for (int i = 0; i < outputs.size(); i++) {
            int size = outputs.get(i).size();
            if (maxSize < size) {
                maxSize = size;
                index = i;
            }
        }
        return index;
    }

    public int chooseMaxInput() {
        int index = 0;
        int maxSize = Integer.MIN_VALUE;
        for (int i = 0; i < outputs.size(); i++) {
            int size = inputs.get(i).size();
            if (maxSize < size) {
                maxSize = size;
                index = i;
            }
        }
        return index;
    }

    public void flushInternal(int index, List<Record> records) {
        List<Node> nodes = context.getClusterContext().getNodesList();
        records.stream()
                .collect(Collectors.groupingBy(record -> record.getHash()))
                .forEach((hash, batch) -> {
                    Node target = nodes.get(hash % nodes.size());
                    engine.send(target, context.getFlowId(), index + 1, new Batch(batch));
                });
    }
}