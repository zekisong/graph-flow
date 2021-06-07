package com.tencent.graphflow;

import com.google.common.collect.Lists;
import com.tencent.graphflow.constant.FlowConstant;
import com.tencent.graphflow.exception.SerializeException;
import com.tencent.graphflow.flow.model.FlowContext;
import com.tencent.graphflow.server.NodeManager;
import java.util.Iterator;

public class Main {

    public static void main(String[] args) throws InterruptedException, SerializeException {
        NodeManager manager = new NodeManager();
        manager.start();

        int start = 1;
        FlowContext<Object, String> context = FlowContext
                .newContext(manager)
                .conf("task.name", "test")
                .conf("flow.context.timeout", 3000L)
                .conf(FlowConstant.TASK_COUNT_PER_EXECUTOR, 10)
                .flatMap(i -> Lists.newArrayList(start * 10, start * 20).iterator())
                .flatMap(i -> Lists.newArrayList(i * 10, i * 20).iterator())
                .flatMap(i -> Lists.newArrayList(i + "1").iterator());
        context.complete();

        Iterator<String> it = manager.getFlowEngine().submit(context);
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        Thread.sleep(100000000);
    }
}