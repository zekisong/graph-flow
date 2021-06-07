package com.tencent.graphflow.flow.runtime;

import com.tencent.graphflow.constant.FlowConstant;
import com.tencent.graphflow.exception.ContextTimeOutException;
import com.tencent.graphflow.exception.FlowRuntimeException;
import com.tencent.graphflow.flow.model.FlowContext;
import com.tencent.graphflow.flow.model.Operator;
import com.tencent.graphflow.server.FlowEngine;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class FlowScheduler<E> implements Iterator {

    private Long timeout;
    private FlowEngine engine;
    private FlowContext context;
    private List<AtomicLong> globalReceives;
    private List<AtomicLong> globalSend;
    private LinkedBlockingQueue<E> results;

    public <S> FlowScheduler(FlowEngine engine, FlowContext<S, E> context) {
        this.engine = engine;
        this.globalReceives = new ArrayList<>();
        this.globalSend = new ArrayList<>();
        this.results = new LinkedBlockingQueue();
        this.context = context;
        this.timeout = context.get(FlowConstant.FLOW_CONTEXT_TIMEOUT, FlowConstant.DEFAULT_FLOW_CONTEXT_TIMEOUT);
        List<Operator> operators = context.getOperators();
        operators.stream().forEach(op -> globalReceives.add(new AtomicLong(0)));
        operators.stream().forEach(op -> globalSend.add(new AtomicLong(0)));
    }

    public void addResult(E result) {
        try {
            results.put(result);
        } catch (InterruptedException e) {
            throw new FlowRuntimeException("add result failed!", e);
        }
    }

    /**
     * TODO sync state
     *
     * @return
     */
    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public E next() {
        try {
            E result = results.poll(timeout, TimeUnit.MILLISECONDS);
            if (result == null) {
                throw new ContextTimeOutException("context timeout");
            }
            return result;
        } catch (InterruptedException e) {
            throw new FlowRuntimeException("take result failed!", e);
        } catch (ContextTimeOutException e) {
            engine.destroy(context.getFlowId());
            throw new FlowRuntimeException(String.format("take result timeout after %d ms!", timeout), e);
        }
    }
}
