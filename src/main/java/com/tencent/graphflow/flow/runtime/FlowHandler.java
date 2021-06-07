package com.tencent.graphflow.flow.runtime;

import com.tencent.graphflow.flow.model.Flow;
import com.tencent.graphflow.flow.model.Operator;
import com.tencent.graphflow.mgmt.ThreadManagement;
import java.util.Iterator;
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
                    if (!flowRuntime.isEmpty()) {
                        ThreadManagement.FLOW.execute(this::run);
                    }
                }
            });
        }
    }

    @Override
    public void run() {
        Record record;
        while ((record = flowRuntime.get()) != null) {
            int opIndex = record.getOpIndex();
            Operator operator = flow.getOperators().get(opIndex);
            Iterator it = operator.process(record.getRecord());
            while (it.hasNext()) {
                Object obj = it.next();
                int hash = operator.hash(obj);
                Record r = new Record(record.getFlowId(), record.getOpIndex() + 1, hash, obj);
                flowRuntime.put(r);
            }
        }
    }
}
