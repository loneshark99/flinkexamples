package com.alexwang.flink.chapter07;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.socketTextStream("localhost", 4567)
                .flatMap((String line, Collector<Tuple2<String, Integer>> collector) -> {
                    String[] words = line.split("\\s+");
                    Arrays.stream(words).map(w -> Tuple2.of(w, 1))
                            .forEach(collector::collect);
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute("word count from socket");
    }
}
