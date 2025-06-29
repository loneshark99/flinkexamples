package com.alexwang.flink.chapter03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class FlinkSlotsSharingGroup {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.socketTextStream("hadoop151", 4546)
                .flatMap((String line, Collector<String> collector) -> {
                    Arrays.stream(line.split("\\s+")).forEach(collector::collect);
                }, Types.STRING)
                .slotSharingGroup("group1")
                .map(String::toUpperCase, Types.STRING)
                .slotSharingGroup("group2")
                .map(word -> Tuple2.of(word, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                .slotSharingGroup("group3")
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        environment.execute("Flink Shared Slots");
    }
}
