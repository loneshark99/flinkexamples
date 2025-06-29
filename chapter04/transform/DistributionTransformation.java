package com.alexwang.flink.chapter04.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DistributionTransformation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        env.socketTextStream("hadoop151", 4567)
                .map(String::toUpperCase)
                .setParallelism(2)
                //.forward()
                //.shuffle()
                //.rebalance()
                //.broadcast()
                //.global()
                //.keyBy(e->e)
                //.rescale()
                .partitionCustom(new MyCustomPartitioner<>(), e -> e)
                .print();
        env.execute("distribution transformation");
    }
}
