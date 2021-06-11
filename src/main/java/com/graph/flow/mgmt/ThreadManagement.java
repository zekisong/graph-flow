package com.graph.flow.mgmt;

import com.graph.flow.config.FlowConfig;
import com.graph.flow.constant.FlowConstant;

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
