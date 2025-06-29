package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.AppAccessLog;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SessionTimeWindowAssigner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] values = line.split(",");
                    return AppAccessLog.builder()
                            .sessionID(values[0])
                            .uri(values[1])
                            .timestamp(Long.parseLong(values[2]))
                            .build();

                }, Types.POJO(AppAccessLog.class))
                .keyBy(AppAccessLog::getSessionID)
                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
                .process(new ProcessWindowFunction<AppAccessLog, Tuple2<String, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String sessionID, Context context, Iterable<AppAccessLog> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        out.collect(Tuple2.of(sessionID, elements.spliterator().estimateSize()));
                    }
                })
                .print();
        env.execute("windowing");
    }
}
