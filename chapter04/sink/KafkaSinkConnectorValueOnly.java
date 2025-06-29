package com.alexwang.flink.chapter04.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KafkaSinkConnectorValueOnly {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(20_000, CheckpointingMode.AT_LEAST_ONCE);
        GeneratorFunction<Long, String> function = (Long num) -> String.format("The generated text line and Num:%s", num);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.88.130:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("flink_kafka_sink")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_generator_source")
                .map(String::toUpperCase, Types.STRING)
                .sinkTo(kafkaSink);

        env.execute();
    }
}
