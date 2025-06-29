package com.alexwang.flink.chapter08;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class SavepointWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop151:8020/flink-checkpoints");
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        checkpointConfig.setCheckpointTimeout(1_000L * 60);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.socketTextStream("hadoop151", 4567)
                .name("socket_read").uid("socket.read")
                .flatMap((String line, Collector<Tuple2<String, Long>> collect) -> {
                    Arrays.stream(line.split("\\s+"))
                            .map(word -> Tuple2.of(word, 1L))
                            .forEach(collect::collect);
                }, Types.TUPLE(Types.STRING, Types.LONG))
                .name("flatmap").uid("flat.map")
                .keyBy(t -> t.f0)
                .sum(1)
                .name("sum").uid("word.count.sum")
                .print().name("print").uid("result.print");

        env.execute("save point");
    }
}
