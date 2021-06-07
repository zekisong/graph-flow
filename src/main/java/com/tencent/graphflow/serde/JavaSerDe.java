package com.tencent.graphflow.serde;

import com.tencent.graphflow.exception.SerializeException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

public class JavaSerDe implements SerDe {

    @Override
    public <T> byte[] serialize(T object) throws SerializeException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput oo = new ObjectOutputStream(bos);
            oo.writeObject(object);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new SerializeException("serialize failed!", e);
        }
    }

    @Override
    public <T> T deSerialize(byte[] data) throws SerializeException {
        try {
            ObjectInput oi = new ObjectInputStream(new ByteArrayInputStream(data));
            T result = (T) oi.readObject();
            return result;
        } catch (Exception e) {
            throw new SerializeException("de serialize failed!", e);
        }
    }
}
