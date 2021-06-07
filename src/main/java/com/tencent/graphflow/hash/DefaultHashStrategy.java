package com.tencent.graphflow.hash;

public class DefaultHashStrategy implements HashStrategy {

    @Override
    public int hash(Object target) {
        return target.hashCode();
    }
}
