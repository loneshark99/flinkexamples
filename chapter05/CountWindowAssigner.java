package com.alexwang.flink.chapter05;

import com.alexwang.flink.model.CityTemperature;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CountWindowAssigner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567).map(line -> {
                    String[] values = line.split(",");
                    return CityTemperature.builder().city(values[0]).temperature(Float.parseFloat(values[1]))
                            .timestamp(Long.parseLong(values[2])).build();
                }, Types.POJO(CityTemperature.class))
                .keyBy(CityTemperature::getCity)
                .countWindow(5)
                .max("temperature")
                .print();

        env.execute("count window");
    }
}
