package com.alexwang.flink.chapter04.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaSinkConnectorKeyValue {

    private final static String TOPIC = "flink_kafka_sink";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(20_000, CheckpointingMode.AT_LEAST_ONCE);
        GeneratorFunction<Long, Tuple2<Integer, String>> function = (Long num) -> {
            return Tuple2.of(num.intValue(), String.format("The generated text line and Num:%s", num));
        };

        DataGeneratorSource<Tuple2<Integer, String>> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                Types.TUPLE(Types.INT, Types.STRING)
        );

        KafkaSink<Tuple2<Integer, String>> kafkaSink = KafkaSink.<Tuple2<Integer, String>>builder()
                .setBootstrapServers("192.168.88.130:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<Tuple2<Integer, String>>builder()
                                .setTopic(TOPIC)
                                .setKeySerializationSchema(new SerializationSchema<Tuple2<Integer, String>>() {
                                    private IntegerSerializer integerSerializer;

                                    @Override
                                    public void open(InitializationContext context) throws Exception {
                                        SerializationSchema.super.open(context);
                                        this.integerSerializer = new IntegerSerializer();
                                    }

                                    @Override
                                    public byte[] serialize(Tuple2<Integer, String> element) {
                                        return this.integerSerializer.serialize(TOPIC, element.f0);
                                    }
                                })
                                .setValueSerializationSchema(new SerializationSchema<Tuple2<Integer, String>>() {
                                    private StringSerializer stringSerializer;

                                    @Override
                                    public void open(InitializationContext context) throws Exception {
                                        SerializationSchema.super.open(context);
                                        this.stringSerializer = new StringSerializer();
                                    }

                                    @Override
                                    public byte[] serialize(Tuple2<Integer, String> element) {
                                        return this.stringSerializer.serialize(TOPIC, element.f1);
                                    }
                                })
                                .build()
                ).setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_generator_source")
                .map(t -> Tuple2.of(t.f0, t.f1.toUpperCase()), Types.TUPLE(Types.INT, Types.STRING))
                .sinkTo(kafkaSink);

        env.execute();
    }
}
