package com.graph.flow;

import com.graph.flow.constant.FlowConstant;
import com.graph.flow.exception.SerializeException;
import com.graph.flow.flow.model.Computer;
import com.graph.flow.flow.model.FlowContext;
import com.graph.flow.server.NodeManager;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Main {

    public static void main(String[] args) throws InterruptedException, SerializeException {
        NodeManager manager = new NodeManager();
        manager.start();

        Computer out = o -> {
            List<Long> nexts = new ArrayList<>();
            for (long i = 0; i < 10; i++) {
                nexts.add((long) (Math.random() * 1000000));
            }
            return nexts.iterator();
        };

        FlowContext<Object, Long> context = FlowContext
                .newContext(manager)
                .conf("task.name", "test")
                .conf("flow.context.timeout", 3000L)
                .conf(FlowConstant.TASK_COUNT_PER_EXECUTOR, 10)
                .flatMap(out)
                .flatMap(out)
                .flatMap(out)
                .flatMap(out);
        context.complete();

        Iterator<Long> it = manager.getFlowEngine().submit(context);
        while (it.hasNext()) {
            it.next();
        }

        Thread.sleep(100000000);
    }
}