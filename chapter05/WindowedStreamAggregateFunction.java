package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.CityTemperature;
import com.alexwang.flink.model.TemperatureStats;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowedStreamAggregateFunction {
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
                .aggregate(new TemperatureStatsAggregateFunction())
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
