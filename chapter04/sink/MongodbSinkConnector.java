package com.alexwang.flink.chapter04.sink;

import com.alexwang.flink.model.User;
import com.mongodb.client.model.InsertOneModel;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.mongodb.sink.MongoSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;

public class MongodbSinkConnector {

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

        MongoSink<BsonDocument> mongoSink = MongoSink.<BsonDocument>builder()
                .setUri("mongodb://127.0.0.1:27017")
                .setDatabase("flink")
                .setCollection("flink_sink")
                .setBatchSize(1_000)
                .setBatchIntervalMs(1_000)
                .setMaxRetries(3)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setSerializationSchema((user, mongoSinkContext) -> new InsertOneModel<>(user))
                .build();


        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_generator_source")
                .map(MongodbSinkConnector::convertToDoc)
                .sinkTo(mongoSink);

        env.execute();
    }

    private static BsonDocument convertToDoc(User user) {
        BsonDocument bsonDocument = new BsonDocument();
        bsonDocument.put("id", new BsonInt32(user.getId()));
        bsonDocument.put("name", new BsonString(user.getName()));

        return bsonDocument;
    }
}
