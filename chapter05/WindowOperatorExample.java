package com.alexwang.flink.chapter05;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowOperatorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] values = line.split("\\s+");
                    return Tuple2.of(values[0], Long.parseLong(values[1]));
                }, Types.TUPLE(Types.STRING, Types.LONG))
                //keyed window
                .keyBy(t->t.f0)
                //time-based
                //1.tumbling window
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                //2. sliding window
                //.window(SlidingProcessingTimeWindows.of(Time.seconds(30),Time.seconds(5)))
                //3. session window
                //.window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
                //4. global window
                //.window(GlobalWindows.create())
                //count-based
                //5. count tumbling window
                //.countWindow(10)
                // count sliding window
                .countWindow(10,3)
                //no keyed window
                //.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .sum(1)
                .print();
        env.execute("windowing");
    }

    /**
     *  final long remainder = (timestamp - offset) % windowSize;
     *         // handle both positive and negative cases
     *         if (remainder < 0) {
     *             return timestamp - (remainder + windowSize);
     *         } else {
     *             return timestamp - remainder;
     *         }
     *
     * Epoch time
     *
     * 30 seconds
     * current time(now)--->1:01
     *
     * 1:00-1:30
     *
     * current time(now)--->1:29
     * 1:00-1:30
     */
}
