package com.alexwang.flink.chapter04.transform;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FilterTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("apache", "flink", "is", "a", "streaming",
                        "data", "process", "framework")
                .filter(e -> e.length() >= 5)
                .print();

        env.execute();
    }
}
