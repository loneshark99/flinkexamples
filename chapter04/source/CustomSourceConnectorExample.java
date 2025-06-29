package com.alexwang.flink.chapter04.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomSourceConnectorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
/*        env.addSource(new SimpleSourceFunction(), "simpleSource")
                .print();*/

        env.addSource(new SimpleParallelSourceFunction(10,100), "simpleSource")
                .setParallelism(4)
                .print().setParallelism(4);

        env.execute("custom source connector");
    }
}
