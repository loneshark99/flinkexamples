package com.alexwang.flink.chapter04.source;

import com.alexwang.flink.model.FlinkUser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public class KafkaSourceConnectorJsonValueOnly {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<FlinkUser> kafkaSource = KafkaSource.<FlinkUser>builder().setBootstrapServers("192.168.88.130:9092")
                .setGroupId("alex_test_json")
                .setTopics("flink_test")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(FlinkUser.class))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("auto.commit.interval.ms", "3000")
                .setProperty("enable.auto.commit", "true")
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .print();

        env.execute("Kafka Source Connector Json Value-Only");
    }
}
