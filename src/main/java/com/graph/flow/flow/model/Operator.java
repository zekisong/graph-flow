package com.graph.flow.flow.model;

import com.graph.flow.hash.DefaultHashStrategy;
import com.graph.flow.hash.HashStrategy;
import java.io.Serializable;
import java.util.Iterator;

public class Operator<I, O> implements Serializable {

    private Computer<I, Iterator<O>> computer;
    private HashStrategy hash;

    public Operator(Computer<I, Iterator<O>> computer, int index) {
        this(computer, new DefaultHashStrategy());
    }

    public Operator(Computer<I, Iterator<O>> computer, HashStrategy hash) {
        this.computer = computer;
        this.hash = hash;
    }

    public Iterator<O> process(I input) {
        return computer.apply(input);
    }

    public int hash(O output) {
        return hash.hash(output);
    }
}
