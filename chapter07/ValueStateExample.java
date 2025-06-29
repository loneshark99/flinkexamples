package com.alexwang.flink.chapter07;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class ValueStateExample {
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
                    private ValueState<Integer> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.valueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Integer>("count", Integer.class)
                        );
                    }

                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        int count = valueState.value() != null ? valueState.value() : 0;
                        valueState.update(++count);
                        out.collect(Tuple2.of(value, valueState.value()));
                    }
                }).print();

        env.execute();
    }
}
