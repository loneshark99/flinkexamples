package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.functions.Partitioner;

public class MyCustomPartitioner<K> implements Partitioner<K> {
    @Override
    public int partition(K key, int numPartitions) {
        return key.hashCode() % numPartitions;
    }
}
