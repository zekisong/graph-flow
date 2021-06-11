package com.graph.flow.hash;

public class DefaultHashStrategy implements HashStrategy {

    @Override
    public int hash(Object target) {
        return target.hashCode();
    }
}
