package com.alexwang.flink.chapter03;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class FlinkOperatorChaining {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);
        //1. disable operator chaining for the streaming operators, this is the global control
        //env.disableOperatorChaining();
        env.socketTextStream("hadoop151", 4567)
                .flatMap((String line, Collector<String> collector) -> {
                    Arrays.stream(line.split("\\s+")).forEach(collector::collect);
                }, Types.STRING)
                .map(word -> Tuple2.of(word, 1L), Types.TUPLE(Types.STRING, Types.LONG))
                //2. since this method applied, the up operator and down operator will not chain together.
                //.disableChaining()
                //3. since this method applied, will start the new chaining begin this operator
                .startNewChain()
                .map(t -> Tuple2.of(t.f0.toUpperCase(), t.f1), Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
