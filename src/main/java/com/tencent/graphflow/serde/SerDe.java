package com.tencent.graphflow.serde;

import com.tencent.graphflow.exception.SerializeException;

public interface SerDe {

    <T> byte[] serialize(T object) throws SerializeException;

    <T> T deSerialize(byte[] data) throws SerializeException;
}