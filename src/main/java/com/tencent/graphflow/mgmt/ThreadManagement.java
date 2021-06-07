package com.tencent.graphflow.mgmt;

import com.tencent.graphflow.config.FlowConfig;
import com.tencent.graphflow.constant.FlowConstant;

public class ThreadManagement {

    public static Threads SCHEDULE;
    public static Threads FLOW;
    public static Threads NETWORK;

    static {
        FlowConfig config = FlowConfig.getInstance();
        SCHEDULE = new Threads(config.get(FlowConstant.THREADS_SCHEDULE_COUNT, FlowConstant.DEFAULT_THREADS_COUNT));
        FLOW = new Threads(config.get(FlowConstant.THREADS_NETWORK_COUNT, FlowConstant.DEFAULT_THREADS_COUNT));
        NETWORK = new Threads(config.get(FlowConstant.THREADS_NETWORK_COUNT, FlowConstant.DEFAULT_THREADS_COUNT));
    }
}
