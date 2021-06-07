package com.tencent.graphflow.flow.runtime;

public interface IO {

    Record get();

    void put(Record record);

}
