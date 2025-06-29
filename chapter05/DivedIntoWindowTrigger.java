package com.alexwang.flink.chapter05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class DivedIntoWindowTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    //<key,num>
                    String[] elements = line.split(",");
                    return Tuple2.of(elements[0], Integer.valueOf(elements[1]));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .trigger(new MyProcessingTimeTrigger())
                .reduce((aggValue, newValue) -> {
                    System.out.println("The reduce method is called.");
                    return Tuple2.of(aggValue.f0, aggValue.f1 + newValue.f1);
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();
        env.execute("windowing");
    }

    private static class MyProcessingTimeTrigger extends Trigger<Tuple2<String, Integer>, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private MyProcessingTimeTrigger() {
        }

        @Override
        public TriggerResult onElement(
                Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) {
            ctx.registerProcessingTimeTimer(window.maxTimestamp());
            System.out.println("The Trigger onElement method is called.");
            if (element.f1 >= 100)
                return TriggerResult.PURGE;
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            throw new RuntimeException("OnEventTime method is never called, because processing time");
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
            System.out.println("The Trigger onProcessingTime method is called.");
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println("The Trigger clear method is called.");
            ctx.deleteProcessingTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) {
            // only register a timer if the time is not yet past the end of the merged window
            // this is in line with the logic in onElement(). If the time is past the end of
            // the window onElement() will fire and setting a timer here would fire the window twice.
            long windowMaxTimestamp = window.maxTimestamp();
            if (windowMaxTimestamp > ctx.getCurrentProcessingTime()) {
                ctx.registerProcessingTimeTimer(windowMaxTimestamp);
            }
        }

        @Override
        public String toString() {
            return "ProcessingTimeTrigger()";
        }

    }

}
