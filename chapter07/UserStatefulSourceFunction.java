package com.alexwang.flink.chapter07;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class UserStatefulSourceFunction {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, "file:///C:\\WorkBench\\Work\\flink\\statestore\\2978094ede92754fc1ecb92a83018e23\\chk-5");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.enableCheckpointing(2000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///C:\\WorkBench\\Work\\flink\\statestore");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.addSource(new StatefulSourceFunction(), "stateful_source")
                .print();
        env.execute();
    }
}
