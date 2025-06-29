package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.CityTemperature;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;

public class GlobalTimeWindowAssigner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567).map(line -> {
                    String[] values = line.split(",");
                    return CityTemperature.builder().city(values[0]).temperature(Float.parseFloat(values[1]))
                            .timestamp(Long.parseLong(values[2])).build();
                }, Types.POJO(CityTemperature.class))
                .keyBy(CityTemperature::getCity)
                .window(GlobalWindows.create())
                .trigger(PurgingTrigger.of(CountTrigger.of(5)))
                .max("temperature")
                .print();

        env.execute("global window");
    }
}
