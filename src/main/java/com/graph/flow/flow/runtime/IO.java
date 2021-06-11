package com.graph.flow.flow.runtime;

public interface IO {

    Batch poll(int opIndex);

    Batch peek(int opIndex);

    void put(int index, Batch record);

}
