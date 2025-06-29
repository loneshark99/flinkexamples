package com.alexwang.flink.chapter08;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

public class SimpleSerializer<T> implements SimpleVersionedSerializer<T> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        }
    }

    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
