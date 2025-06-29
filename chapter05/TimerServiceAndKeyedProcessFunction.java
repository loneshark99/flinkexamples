package com.alexwang.flink.chapter05;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TimerServiceAndKeyedProcessFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] elements = line.split(",");
                    return Tuple2.of(elements[0], Integer.valueOf(elements[1]));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

                    private ValueState<Integer> valueState;
                    private ValueState<Long> timestampState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Types.INT));
                        this.timestampState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timestampstate", Types.LONG));
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        if (valueState.value() == null) {
                            valueState.update(0);
                        }
                        if (timestampState.value() == null) {
                            timestampState.update(ctx.timerService().currentProcessingTime() + 1_000 * 60L);
                            ctx.timerService().registerProcessingTimeTimer(timestampState.value());
                        }
                        valueState.update(valueState.value() + value.f1);
                        //no long emit the result to downstream
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        out.collect(Tuple2.of(ctx.getCurrentKey(), valueState.value()));
                        ctx.timerService().deleteProcessingTimeTimer(timestampState.value());
                        timestampState.update(null);
                    }
                })
                .print();
        env.execute();
    }
}
