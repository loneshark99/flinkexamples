package com.alexwang.flink.chapter04.sink;

import com.alexwang.flink.model.User;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class KafkaSinkConnectorJson {

    private final static String TOPIC = "flink_kafka_sink_json";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(20_000, CheckpointingMode.AT_LEAST_ONCE);
        GeneratorFunction<Long, User> function = (Long num) -> User.builder()
                .id(num.intValue()).name("Alex-" + num).build();

        DataGeneratorSource<User> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                Types.POJO(User.class)
        );

        KafkaSink<User> kafkaSink = KafkaSink.<User>builder()
                .setBootstrapServers("192.168.88.130:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<User>builder()
                                .setTopic(TOPIC)
                                .setKeySerializationSchema(new SerializationSchema<User>() {
                                    private IntegerSerializer integerSerializer;

                                    @Override
                                    public void open(InitializationContext context) throws Exception {
                                        SerializationSchema.super.open(context);
                                        this.integerSerializer = new IntegerSerializer();
                                    }

                                    @Override
                                    public byte[] serialize(User element) {
                                        return this.integerSerializer.serialize(TOPIC, element.getId());
                                    }
                                })
                                .setValueSerializationSchema(new JsonSerializationSchema<>())
                                .build()
                ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_generator_source")
                .sinkTo(kafkaSink);

        env.execute();
    }
}
