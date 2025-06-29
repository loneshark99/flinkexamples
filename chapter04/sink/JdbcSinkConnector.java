package com.alexwang.flink.chapter04.sink;

import com.alexwang.flink.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class JdbcSinkConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(2_000, CheckpointingMode.EXACTLY_ONCE);

        GeneratorFunction<Long, Person> function = (Long num) -> Person.builder().id(num.intValue())
                .age(num.intValue())
                .address("address " + num).name("name " + num).build();
        DataGeneratorSource<Person> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                Types.POJO(Person.class)
        );

        SinkFunction<Person> jdbcSink = JdbcSink.<Person>sink(
                "insert into person(id,age,name,address) values(?,?,?,?)",
                (st, person) -> {
                    st.setInt(1, person.getId());
                    st.setInt(2, person.getAge());
                    st.setString(3, person.getName());
                    st.setString(4, person.getAddress());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(10)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://192.168.88.130:3306/flink")
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("root1234")
                        .withConnectionCheckTimeoutSeconds(60)
                        .build()

        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "data_generator")
                .addSink(jdbcSink);

        env.execute();
    }
}
