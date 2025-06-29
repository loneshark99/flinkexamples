package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.CityTemperature;
import com.alexwang.flink.model.TemperatureStats;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.DoubleSummaryStatistics;
import java.util.stream.StreamSupport;

public class WindowedStreamProcessWindowFunction {
    private final static String FORMATTER = "HH:mm:ss.SSS";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] values = line.split(",");
                    return CityTemperature.builder()
                            .city(values[0])
                            .temperature(Float.parseFloat(values[1]))
                            .timestamp(Long.parseLong(values[2]))
                            .build();

                }, Types.POJO(CityTemperature.class))
                .keyBy(CityTemperature::getCity)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .process(new ProcessWindowFunction<CityTemperature, TemperatureStats, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<CityTemperature> elements, Collector<TemperatureStats> out) throws Exception {
                        final TimeWindow window = context.window();
                        String start = DateFormatUtils.format(window.getStart(), FORMATTER);
                        String end = DateFormatUtils.format(window.getEnd(), FORMATTER);
                        long count = elements.spliterator().estimateSize();
                        System.out.println(String.format("key:%s,count:%s,window[%s-%s)", key, count, start, end));

                        DoubleSummaryStatistics statistics = StreamSupport.stream(elements.spliterator(), false)
                                .map(CityTemperature::getTemperature)
                                .mapToDouble(Float::doubleValue)
                                .summaryStatistics();
                        TemperatureStats stats = TemperatureStats.builder()
                                .city(key)
                                .max((float) statistics.getMax())
                                .min((float) statistics.getMin())
                                .average((float) statistics.getAverage())
                                .build();

                        out.collect(stats);
                    }
                })
                .print();
        env.execute("windowing");
    }
}
