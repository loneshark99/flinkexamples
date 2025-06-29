package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.CityTemperature;
import com.alexwang.flink.model.TemperatureStats;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowedStreamAggregateAndProcessFunction {
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
                .aggregate(new TemperatureStatsAggregateFunction(), new ProcessWindowFunction<TemperatureStats, TemperatureStats, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<TemperatureStats> elements, Collector<TemperatureStats> out) throws Exception {
                        System.out.println("elements count:" + elements.spliterator().estimateSize());
                        final TimeWindow window = context.window();
                        String start = DateFormatUtils.format(window.getStart(), FORMATTER);
                        String end = DateFormatUtils.format(window.getEnd(), FORMATTER);
                        long count = elements.spliterator().estimateSize();
                        System.out.println(String.format("key:%s,count:%s,window[%s-%s)", key, count, start, end));
                        out.collect(elements.iterator().next());
                    }
                })
                .print();
        env.execute("windowing");
    }

    private static class TemperatureStatsAggregateFunction implements AggregateFunction<CityTemperature, TemperatureStats, TemperatureStats> {

        @Override
        public TemperatureStats createAccumulator() {
            System.out.println("the createAccumulator method is called");
            return TemperatureStats.builder()
                    .average(Float.MIN_VALUE)
                    .min(Float.MAX_VALUE)
                    .max(Float.MIN_VALUE)
                    .build();
        }

        @Override
        public TemperatureStats add(CityTemperature value, TemperatureStats accumulator) {
            System.out.println("the add method is called, newValue:" + value + ",accumulator:" + accumulator);
            TemperatureStats temperatureStats = new TemperatureStats();
            temperatureStats.setCity(value.getCity());
            temperatureStats.setMin(Math.min(accumulator.getMin(), value.getTemperature()));
            temperatureStats.setMax(Math.max(accumulator.getMax(), value.getTemperature()));
            temperatureStats.setAverage((accumulator.getAverage() == Float.MIN_VALUE)
                    ? value.getTemperature() : (accumulator.getAverage() + value.getTemperature()) / 2);
            return temperatureStats;
        }

        @Override
        public TemperatureStats getResult(TemperatureStats accumulator) {
            return TemperatureStats.build(accumulator);
        }

        @Override
        public TemperatureStats merge(TemperatureStats a, TemperatureStats b) {

            return TemperatureStats.builder()
                    .city(a.getCity())
                    .min(Math.min(a.getMin(), b.getMin()))
                    .max(Math.max(a.getMax(), b.getMax()))
                    .average((a.getAverage() + b.getAverage()) / 2)
                    .build();
        }
    }
}
