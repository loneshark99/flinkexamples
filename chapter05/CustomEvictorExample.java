package com.alexwang.flink.chapter05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;

import java.util.Iterator;

public class CustomEvictorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] elements = line.split(",");
                    return Tuple2.of(elements[0], Integer.valueOf(elements[1]));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .evictor(new MyEvictor())
                .reduce((aggValue, newValue) -> Tuple2.of(aggValue.f0, aggValue.f1 + newValue.f1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();
        env.execute();
    }

    private static class MyEvictor implements Evictor<Tuple2<String, Integer>, TimeWindow> {

        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, TimeWindow window,
                                EvictorContext evictorContext) {
            Iterator<TimestampedValue<Tuple2<String, Integer>>> it = elements.iterator();
            while (it.hasNext()) {
                TimestampedValue<Tuple2<String, Integer>> value = it.next();
                if (value.getValue().f1 < 0) {
                    it.remove();
                }
            }
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, TimeWindow window,
                               EvictorContext evictorContext) {
            //do nothing
        }
    }
}
