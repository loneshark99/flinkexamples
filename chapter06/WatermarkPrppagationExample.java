package com.alexwang.flink.chapter06;

import com.alexwang.flink.model.CityTemperature;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class WatermarkPrppagationExample {
    private final static String FORMATTER = "HH:mm:ss.SSS";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] values = line.split(",");
                    return CityTemperature.builder()
                            .city(values[0])
                            .temperature(Float.parseFloat(values[1]))
                            .timestamp(Long.parseLong(values[2]))
                            .build();
                }, Types.POJO(CityTemperature.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<CityTemperature>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(context -> ((element, recordTimestamp) -> element.getTimestamp())))
                .keyBy(CityTemperature::getCity)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<CityTemperature, String, String, TimeWindow>() {

                    @Override
                    public void process(String key, Context context, Iterable<CityTemperature> elements, Collector<String> out) throws Exception {
                        final StringBuilder builder = new StringBuilder();
                        final TimeWindow window = context.window();
                        final String start = DateFormatUtils.format(window.getStart(), FORMATTER);
                        final String end = DateFormatUtils.format(window.getEnd(), FORMATTER);
                        builder.append("key:").append(key).append(",[").append(start)
                                .append("-").append(end).append(")maxTimestamp:")
                                .append(DateFormatUtils.formatUTC(window.maxTimestamp(), FORMATTER))
                                .append(",count:").append(elements.spliterator().estimateSize())
                                .append(",").append(elements);
                        out.collect(builder.toString());
                    }
                })

                .print();
        env.execute("windowing");
    }
}
