package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.CityTemperature;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowedStreamReduceFunction {

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
                .reduce((aggValue, newValue) -> {
                    System.out.println("aggValue:" + aggValue + ",newValue:" + newValue);
                    float averageTemperature = (aggValue.getTemperature() + newValue.getTemperature()) / 2;
                    return CityTemperature.builder()
                            .city(aggValue.getCity())
                            .temperature(averageTemperature)
                            .timestamp(newValue.getTimestamp())
                            .build();
                })
                .print();
        env.execute("windowing");
    }
}
