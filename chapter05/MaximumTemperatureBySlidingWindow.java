package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.CityTemperature;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//Sliding Processing Time Window Assigner
public class MaximumTemperatureBySlidingWindow {

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
                .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
                .max("temperature")
                .print();
        env.execute("windowing");
    }
}
