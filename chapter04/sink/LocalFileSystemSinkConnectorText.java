package com.alexwang.flink.chapter04.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.File;
import java.time.Duration;

public class LocalFileSystemSinkConnectorText {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(2_000, CheckpointingMode.EXACTLY_ONCE);
        GeneratorFunction<Long, String> function = (Long num) -> String.format("The generated text line and No.: %s", num);
        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );

        FileSink<String> fileSink = FileSink.<String>forRowFormat(Path.fromLocalFile(new File("C:\\WorkBench\\Work\\flink")),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(MemorySize.parse("1024", MemorySize.MemoryUnit.BYTES))
                        .withRolloverInterval(Duration.ofSeconds(30))
                        .build())
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd-HH"))
                .withOutputFileConfig(new OutputFileConfig("alex", ".txt"))
                .build();
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_generator_source")
                .map(String::toUpperCase, Types.STRING)
                .sinkTo(fileSink);

        env.execute();
    }
}
