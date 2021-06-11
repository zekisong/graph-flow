package com.graph.flow.serde;

import com.graph.flow.exception.SerializeException;

public interface SerDe {

    <T> byte[] serialize(T object) throws SerializeException;

    <T> T deSerialize(byte[] data) throws SerializeException;
}