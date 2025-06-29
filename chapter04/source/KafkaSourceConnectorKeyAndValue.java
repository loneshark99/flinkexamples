package com.alexwang.flink.chapter04.source;

import com.alexwang.flink.model.FlinkUser;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.jackson.JacksonMapperFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;

public class KafkaSourceConnectorKeyAndValue {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<Tuple2<Integer,FlinkUser>> kafkaSource = KafkaSource.<Tuple2<Integer,FlinkUser>>builder().setBootstrapServers("192.168.88.130:9092")
                .setGroupId("alex_test_key_value")
                .setTopics("flink_test2")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new KafkaRecordDeserializationSchema<Tuple2<Integer, FlinkUser>>() {

                    private ObjectMapper objectMapper;

                    @Override
                    public void open(DeserializationSchema.InitializationContext context) throws Exception {
                        this.objectMapper = JacksonMapperFactory.createObjectMapper();
                    }

                    @Override
                    public TypeInformation<Tuple2<Integer, FlinkUser>> getProducedType() {
                        return new TupleTypeInfo<>(Types.INT, Types.POJO(FlinkUser.class));
                    }

                    @Override
                    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<Tuple2<Integer, FlinkUser>> collector)
                            throws IOException {
                        int key = ByteBuffer.wrap(consumerRecord.key()).getInt();
                        FlinkUser flinkUser = this.objectMapper.readValue(consumerRecord.value(), FlinkUser.class);
                        collector.collect(Tuple2.of(key, flinkUser));
                    }
                })
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("auto.commit.interval.ms", "3000")
                .setProperty("enable.auto.commit", "true")
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .print();

        env.execute("Kafka Source Connector Key And Value");
    }
}
