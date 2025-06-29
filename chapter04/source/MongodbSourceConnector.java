package com.alexwang.flink.chapter04.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.mongodb.source.MongoSource;
import org.apache.flink.connector.mongodb.source.enumerator.splitter.PartitionStrategy;
import org.apache.flink.connector.mongodb.source.reader.deserializer.MongoDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.bson.BsonDocument;

import java.io.IOException;
import java.util.Arrays;

public class MongodbSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        MongoSource<String> mongoSource = MongoSource.<String>builder()
                .setUri("mongodb://127.0.0.1:27017")
                .setDatabase("flink")
                .setCollection("flink_source")
                .setProjectedFields("_id", "id", "name", "age", "address")
                .setFetchSize(2048)
                .setLimit(100)
                .setNoCursorTimeout(true)
                .setPartitionStrategy(PartitionStrategy.SAMPLE)
                .setDeserializationSchema(new MongoDeserializationSchema<String>() {
                    @Override
                    public String deserialize(BsonDocument bsonDocument) throws IOException {
                        return bsonDocument.toJson();
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return Types.STRING;
                    }
                }).build();

        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "mongo-source")
                .print();

        env.execute("Mongodb source connector");
    }
}
