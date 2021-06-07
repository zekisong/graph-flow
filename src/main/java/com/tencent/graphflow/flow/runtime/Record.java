package com.tencent.graphflow.flow.runtime;

import java.io.Serializable;

public class Record<T> implements Serializable, Comparable {

    private Long flowId;
    private int opIndex;
    private T record;
    private transient int hash;

    public Record() {
    }

    public Record(Long flowId, T record) {
        this.flowId = flowId;
        this.record = record;
    }

    public Record(Long flowId, int opIndex, int hash, T record) {
        this.flowId = flowId;
        this.opIndex = opIndex;
        this.hash = hash;
        this.record = record;
    }

    public int getOpIndex() {
        return opIndex;
    }

    public Long getFlowId() {
        return flowId;
    }

    public T getRecord() {
        return record;
    }

    public int getHash() {
        return hash;
    }

    public void setHash(int hash) {
        this.hash = hash;
    }

    @Override
    public int compareTo(Object o) {
        return 0;
    }
}
