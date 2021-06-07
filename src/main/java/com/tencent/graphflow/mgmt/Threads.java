package com.tencent.graphflow.mgmt;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Threads {

    private ThreadPoolExecutor threads;

    public Threads() {
        this(Runtime.getRuntime().availableProcessors());
    }

    public Threads(int coreSize) {
        threads = new ThreadPoolExecutor(coreSize, coreSize,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>());
    }

    public void execute(Runnable runnable) {
        threads.execute(runnable);
    }

    public ExecutorService getThreads() {
        return threads;
    }
}
