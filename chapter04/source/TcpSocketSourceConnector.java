package com.alexwang.flink.chapter04.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TcpSocketSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);
        env.socketTextStream("hadoop151", 4567, "\n").setParallelism(2)
                .print();
        env.execute("TcpSourceSourceCollectorExample");
    }
}
