package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class FlatMapTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("white", "black", "white", "gray", "black", "white")
                .flatMap((String color, Collector<String> collector) -> {
                    switch (color) {
                        case "white":
                            collector.collect(color.toUpperCase());
                            break;
                        case "black":
                            collector.collect(color);
                            collector.collect(color);
                            break;
                        default:
                            break;
                    }
                }, Types.STRING)
                .print();

        env.execute();
    }
}
