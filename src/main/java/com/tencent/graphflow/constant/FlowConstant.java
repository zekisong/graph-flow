package com.tencent.graphflow.constant;

public class FlowConstant {

    public static final String CLUSTER_ZK_ADDR = "cluster.zk.addr";
    public static final String DEFAULT_CLUSTER_ZK_ADDR = "127.0.0.1:2181";
    public static final String CLUSTER_ZK_TIMEOUT = "cluster.zk.timeout";
    public static final Integer DEFAULT_CLUSTER_ZK_TIMEOUT = 60 * 1000;
    public static final String CLUSTER_NAME = "cluster.zk.path";
    public static final String DEFAULT_CLUSTER_NAME = "flow";
    public static final String TASK_COUNT_PER_EXECUTOR = "task.count.per.executor";
    public static final Integer DEFAULT_TASK_COUNT_PER_EXECUTOR = 10;
    public static final String THREADS_SCHEDULE_COUNT = "threads.schedule.count";
    public static final String THREADS_FLOW_COUNT = "threads.flow.count";
    public static final String THREADS_NETWORK_COUNT = "threads.network.count";
    public static final Integer DEFAULT_THREADS_COUNT = Runtime.getRuntime().availableProcessors();
    public static final String FLOW_CONTEXT_TIMEOUT = "flow.context.timeout";
    public static final Long DEFAULT_FLOW_CONTEXT_TIMEOUT = 1000L;
}
