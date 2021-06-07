package com.tencent.graphflow.flow.runtime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Lists;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Batch {

    private static final Logger LOG = LoggerFactory.getLogger(Batch.class);
    private static ThreadLocal<Kryo> kryo = new ThreadLocal();

    public static <T extends Record> byte[] encode(List<T> records) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        getKryo().writeObjectOrNull(output, records, ArrayList.class);
        output.flush();
        return baos.toByteArray();
    }

    public static <T extends Record> List<T> decode(byte[] data) {
        try {
            if (data.length == 0) {
                return Lists.newArrayList();
            } else {
                return getKryo().readObject(new Input(data, 0, data.length), ArrayList.class);
            }
        } catch (Exception e) {
            LOG.error("decode failed!", e);
            throw new RuntimeException("decode failed!", e);
        }
    }

    public static Kryo getKryo() {
        Kryo instance = kryo.get();
        if (instance == null) {
            synchronized (Batch.class) {
                if (kryo.get() == null) {
                    instance = new Kryo();
                    kryo.set(instance);
                }
            }
        }
        return instance;
    }
}
