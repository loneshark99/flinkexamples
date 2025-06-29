package com.alexwang.flink.chapter08;

import com.alexwang.flink.model.CityTemperature;
import com.alexwang.flink.model.TemperatureStats;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.Duration;
import java.util.DoubleSummaryStatistics;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class FlinkWithKafkaEosExample {
    private static final String SOURCE_TOPIC = "city_temperature";
    private static final String TARGET_TOPIC = "temperature_stats";

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, "file:///C:\\WorkBench\\Work\\flink\\statestore\\587c9d90fb5c9f84747adf5c3651f171\\chk-39");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.enableCheckpointing(5_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setCheckpointStorage("file:///C:\\WorkBench\\Work\\flink\\statestore");
        checkpointConfig.enableUnalignedCheckpoints(false);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setCheckpointTimeout(10_000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        KafkaSource<CityTemperature> kafkaSource = KafkaSource.<CityTemperature>builder().setBootstrapServers("192.168.88.130:9092")
                .setGroupId("flink_kafka_eos_test")
                .setTopics(SOURCE_TOPIC)
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(CityTemperature.class))
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("auto.commit.interval.ms", "5000")
                .setProperty("enable.auto.commit", "true")
                .build();

        Properties properties = new Properties();
        properties.setProperty("transaction.timeout.ms", "60000");
        KafkaSink<TemperatureStats> kafkaSink = KafkaSink.<TemperatureStats>builder()
                .setBootstrapServers("192.168.88.130:9092")
                .setKafkaProducerConfig(properties)
                .setRecordSerializer(KafkaRecordSerializationSchema.<TemperatureStats>builder()
                        .setTopic(TARGET_TOPIC)
                        .setValueSerializationSchema(new JsonSerializationSchema<>())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("my-trx-id-prefix")
                .build();

        env.fromSource(kafkaSource, WatermarkStrategy.<CityTemperature>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withIdleness(Duration.ofSeconds(1))
                                .withTimestampAssigner(context -> ((element, recordTimestamp) -> element.getTimestamp()))
                        , "kafka_source"
                ).name("kafka_source").uid("city.temperature.kafka.source")
                .keyBy(CityTemperature::getCity)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<CityTemperature, TemperatureStats, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<CityTemperature> elements, Collector<TemperatureStats> out) throws Exception {
                        DoubleSummaryStatistics statistics = StreamSupport.stream(elements.spliterator(), false)
                                .mapToDouble(CityTemperature::getTemperature)
                                .summaryStatistics();
                        out.collect(TemperatureStats.builder()
                                .city(key)
                                .max((float) statistics.getMax())
                                .min((float) statistics.getMin())
                                .average((float) statistics.getAverage())
                                .build());
                    }
                }).name("stats_temperature").uid("stats.temperature")
                .sinkTo(kafkaSink).name("stats_kafka_sink").uid("stats.kafka.sink");
        env.execute("EOS");
    }
}
