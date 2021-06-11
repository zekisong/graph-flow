package com.graph.flow.flow.runtime;

import com.graph.flow.flow.model.Flow;
import com.graph.flow.flow.model.Operator;
import com.graph.flow.mgmt.ThreadManagement;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class FlowHandler implements Runnable {

    private AtomicReference<ThreadState> state;
    private FlowRuntime flowRuntime;
    private Flow flow;

    public FlowHandler(FlowRuntime flowRuntime, Flow flow) {
        this.flowRuntime = flowRuntime;
        this.flow = flow;
        this.state = new AtomicReference<>(ThreadState.IDLE);
    }

    public void activation() {
        if (state.compareAndSet(ThreadState.IDLE, ThreadState.RUNNING)) {
            ThreadManagement.FLOW.execute(() -> {
                try {
                    run();
                } finally {
                    state.compareAndSet(ThreadState.RUNNING, ThreadState.IDLE);
                    if (flowRuntime.hasInput()) {
                        ThreadManagement.FLOW.execute(this::activation);
                    }
                }
            });
        }
    }

    @Override
    public void run() {
        int count = 0;
        int index = flowRuntime.chooseMaxInput();
        while (true) {
            Batch batch = flowRuntime.poll(index);
            if (batch == null) {
                break;
            }
            Operator operator = flow.getOperators().get(index);
            List<Record> records = batch.get();
            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);
                Iterator it = operator.process(record.getRecord());
                Batch next = new Batch();
                while (it.hasNext()) {
                    Object result = it.next();
                    int hash = operator.hash(result);
                    Record r = new Record(hash, result);
                    next.add(r);
                    count++;
                }
                flowRuntime.put(index, next);
               /* if (count > 10000) {
                    flowRuntime.input(index, new Batch(records.subList(i, records.size())));
                    break;
                }*/
            }
        }
    }

}
