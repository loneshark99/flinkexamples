package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        GeneratorFunction<Long, Integer> function = Long::intValue;
        DataGeneratorSource<Integer> source = new DataGeneratorSource<>(
                function,
                100,
                RateLimiterStrategy.perSecond(5),
                Types.INT
        );

        final OutputTag<String> oddDigitalTag = new OutputTag<String>("odd") {
        };
        SingleOutputStreamOperator<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "data generate")
                .process(new ProcessFunction<Integer, String>() {
                    @Override
                    public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                        if (value % 2 == 1) {
                            ctx.output(oddDigitalTag, "side output " + value);
                        } else {
                            out.collect("main output " + value);
                        }
                    }
                });
        SideOutputDataStream<String> sideOutput = dataStream.getSideOutput(oddDigitalTag);
        dataStream.print();
        sideOutput.map(String::toUpperCase, Types.STRING).print();

        env.execute("side output");
    }
}
