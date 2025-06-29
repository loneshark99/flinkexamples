package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements("apache", "flink", "is", "a", "streaming", "data", "process", "framework")
                .map(String::toUpperCase, Types.STRING)
                .print();
        env.execute();
    }
}
