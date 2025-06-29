package com.alexwang.flink.chapter05;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import java.io.IOException;

public class GlobalWindowAndCustomTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] elements = line.split(",");
                    return Tuple2.of(elements[0], Integer.valueOf(elements[1]));
                }, Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .window(GlobalWindows.create())
                .trigger(new MyProcessingTimeTrigger(3, Time.seconds(5)))
                .reduce((aggValue, newValue) -> Tuple2.of(aggValue.f0, aggValue.f1 + newValue.f1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .print();
        env.execute();
    }

    private static class MyProcessingTimeTrigger extends Trigger<Tuple2<String, Integer>, GlobalWindow> {
        private final long maxCount;
        private final long timeoutMillSeconds;
        private final ValueStateDescriptor<Long> countDesc;
        private final ValueStateDescriptor<Long> triggerTimeDesc;

        private MyProcessingTimeTrigger(long maxCount, Time time) {
            this.maxCount = maxCount;
            this.timeoutMillSeconds = time.toMilliseconds();
            this.countDesc = new ValueStateDescriptor<Long>("count", Types.LONG);
            this.triggerTimeDesc = new ValueStateDescriptor<Long>("triggerTime", Types.LONG);
        }

        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx)
                throws Exception {
            System.out.println("The trigger onElement method is called.");
            ValueState<Long> countState = ctx.getPartitionedState(countDesc);
            ValueState<Long> triggerTimeState = ctx.getPartitionedState(triggerTimeDesc);
            long currentCount = getValue(countState) + 1;
            if (currentCount == 1) {
                //init
                System.out.println("New Trigger start in the new window");
                long triggerTimestamp = ctx.getCurrentProcessingTime() + timeoutMillSeconds;
                ctx.registerProcessingTimeTimer(triggerTimestamp);
                triggerTimeState.update(triggerTimestamp);
            }

            countState.update(currentCount);

            if (countState.value() >= maxCount) {
                return this.doFireAndPurge(ctx, countState, triggerTimeState);
            }

            if ((getValue(triggerTimeState) < ctx.getCurrentProcessingTime()) && getValue(countState) > 0) {
                return this.doFireAndPurge(ctx, countState, triggerTimeState);
            }
            return TriggerResult.CONTINUE;
        }

        private TriggerResult doFireAndPurge(TriggerContext ctx, ValueState<Long> countState, ValueState<Long> triggerTimeState)
                throws IOException {
            ctx.deleteProcessingTimeTimer(getValue(triggerTimeState));
            countState.update(0L);
            triggerTimeState.update(0L);
            return TriggerResult.FIRE_AND_PURGE;
        }

        private long getValue(ValueState<Long> valueState) throws IOException {
            return valueState.value() == null ? 0L : valueState.value();
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            System.out.println("Trigger onProcessingTime method is called.");
            return this.doFireAndPurge(ctx, ctx.getPartitionedState(countDesc), ctx.getPartitionedState(triggerTimeDesc));
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
            System.out.println("Trigger clear method is called.");
            ctx.getPartitionedState(countDesc).clear();
            ctx.deleteProcessingTimeTimer(getValue(ctx.getPartitionedState(triggerTimeDesc)));
            ctx.getPartitionedState(triggerTimeDesc).clear();

        }
    }
}
