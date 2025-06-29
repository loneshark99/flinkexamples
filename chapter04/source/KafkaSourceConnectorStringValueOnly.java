package com.alexwang.flink.chapter04.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaSourceConnectorStringValueOnly {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("192.168.88.130:9092")
                .setGroupId("alex_test")
                .setTopics("flink_test")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("auto.commit.interval.ms", "3000")
                .setProperty("enable.auto.commit", "true")
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .print();

        env.execute("Kafka Source Connector String Value-Only");
    }
}
