package com.graph.flow.flow.runtime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Batch<T> {

    private static final Logger LOG = LoggerFactory.getLogger(Batch.class);
    private static ThreadLocal<Kryo> kryo = new ThreadLocal();
    private List<Record<T>> records;

    public Batch() {
        records = new ArrayList<>();
    }

    public Batch(List<Record<T>> records) {
        this.records = records;
    }

    public void add(Record<T> record) {
        this.records.add(record);
    }

    public int size() {
        return this.records.size();
    }

    public List<Record<T>> get() {
        return this.records;
    }

    public byte[] encode() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output output = new Output(baos);
        getKryo().writeObjectOrNull(output, this, Batch.class);
        output.flush();
        return baos.toByteArray();
    }

    public static Batch decode(byte[] data) {
        try {
            if (data.length == 0) {
                return new Batch<>();
            } else {
                return getKryo().readObject(new Input(data, 0, data.length), Batch.class);
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
