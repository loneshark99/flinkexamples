package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyByTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        env.fromElements(Tuple2.of("Apple", 300.0D), Tuple2.of("Mic", 365.0D), Tuple2.of("Google", 1500.0D),
                        Tuple2.of("Apple", 310.0D), Tuple2.of("Mic", 355.0D), Tuple2.of("Google", 1400.0D))
                .keyBy(t -> t.f0)
                .max(1)
                .print();
        env.execute("KeyBy");
    }
}
