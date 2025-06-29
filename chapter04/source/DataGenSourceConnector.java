package com.alexwang.flink.chapter04.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataGenSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(2);
        GeneratorFunction<Long, String> function = e -> "Elements-> " + String.valueOf(e);
        DataGeneratorSource<String> generatorSource = new DataGeneratorSource<>(function, 100,
                RateLimiterStrategy.perSecond(10),
                Types.STRING);
        env.fromSource(generatorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource")
                .print();

        env.execute("DataGenSourceCollectorExample");
    }
}
