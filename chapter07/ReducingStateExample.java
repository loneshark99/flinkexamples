package com.alexwang.flink.chapter07;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class ReducingStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.socketTextStream("localhost", 4567)
                .flatMap((String line, Collector<String> collector) -> {
                    String[] words = line.split("\\s+");
                    Arrays.stream(words).forEach(collector::collect);
                }, Types.STRING)
                .keyBy(e -> e)
                .process(new KeyedProcessFunction<String, String, Tuple2<String, Integer>>() {
                    private ReducingState<Integer> reducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>(
                                "count",
                                Integer::sum,
                                Integer.class
                        ));
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        this.reducingState.add(1);
                        out.collect(Tuple2.of(value, reducingState.get()));
                    }
                }).print();

        env.execute();
    }
}
