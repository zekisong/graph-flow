package com.graph.flow.flow.runtime;

import com.graph.flow.constant.FlowConstant;
import com.graph.flow.exception.ContextTimeOutException;
import com.graph.flow.exception.FlowRuntimeException;
import com.graph.flow.flow.model.FlowContext;
import com.graph.flow.flow.model.Operator;
import com.graph.flow.server.FlowEngine;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlowScheduler<E> implements Iterator {

    private static final Logger LOG = LoggerFactory.getLogger(FlowScheduler.class);
    private Long timeout;
    private Long start = System.currentTimeMillis();
    private FlowEngine engine;
    private FlowContext context;
    private List<AtomicLong> globalReceives;
    private List<AtomicLong> globalSend;
    private LinkedBlockingQueue<E> results;
    private AtomicLong resultCount = new AtomicLong();

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
        results.add(result);
        resultCount.incrementAndGet();
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
            LOG.info("use time:" + (System.currentTimeMillis() - start) + " result count:" + resultCount.get());
            throw new FlowRuntimeException(String.format("take result timeout after %d ms!", timeout), e);
        }
    }
}
