package com.graph.flow.flow.runtime;

import java.io.Serializable;

public class Record<T> implements Serializable {

    private T record;
    private transient int hash;

    public Record() {
    }

    public Record(T record) {
        this.record = record;
    }

    public Record(int hash, T record) {
        this.hash = hash;
        this.record = record;
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
}
