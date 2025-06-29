package com.alexwang.flink.chapter04.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

import java.io.File;

public class LocalFileSystemSinkConnectorParquet {
    private final static String SCHEMA = "{\n" +
            "  \"namespace\": \"example.avro\",\n" +
            "  \"type\": \"record\",\n" +
            "  \"name\": \"User\",\n" +
            "  \"fields\": [\n" +
            "    {\n" +
            "      \"name\": \"name\",\n" +
            "      \"type\": \"string\"\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"favorite_number\",\n" +
            "      \"type\": [\n" +
            "        \"int\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    },\n" +
            "    {\n" +
            "      \"name\": \"favorite_color\",\n" +
            "      \"type\": [\n" +
            "        \"string\",\n" +
            "        \"null\"\n" +
            "      ]\n" +
            "    }\n" +
            "  ]\n" +
            "}";

    private final static Schema AVRO_SCHEMA = new Schema.Parser().parse(SCHEMA);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(2_000, CheckpointingMode.EXACTLY_ONCE);
        GeneratorFunction<Long, GenericRecord> function = (Long num) -> {
            GenericData.Record record = new GenericData.Record(AVRO_SCHEMA);
            record.put("name", "Alex");
            record.put("favorite_number", num.intValue());
            record.put("favorite_color", "black");
            return record;
        };

        DataGeneratorSource<GenericRecord> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                new GenericRecordAvroTypeInfo(AVRO_SCHEMA)
        );

        FileSink<GenericRecord> fileSink = FileSink.<GenericRecord>forBulkFormat(Path.fromLocalFile(new File("C:\\WorkBench\\Work\\flink")),
                        AvroParquetWriters.forGenericRecord(AVRO_SCHEMA))
                .withRollingPolicy(
                        OnCheckpointRollingPolicy.build()
                )
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd-HH"))
                .withOutputFileConfig(new OutputFileConfig("alex", ".parquet"))
                .build();

        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_generator_source")
                .sinkTo(fileSink);

        env.execute();
    }
}
