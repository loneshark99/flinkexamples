package com.alexwang.flink.chapter04.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSinkConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("hadoop151", 4567)
                .map(String::toUpperCase, Types.STRING)
                .writeToSocket("hadoop151", 4568, new SimpleStringSchema());

        env.execute();
    }
}
