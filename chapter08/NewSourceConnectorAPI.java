package com.alexwang.flink.chapter08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class NewSourceConnectorAPI {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(SavepointConfigOptions.SAVEPOINT_PATH, "file:///C:\\WorkBench\\Work\\flink\\statestore\\8d8b3da6cdf20bd06b9c04609520e77d\\chk-9");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.enableCheckpointing(5_000L, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(10_000L);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.enableUnalignedCheckpoints(false);
        env.setParallelism(3);
        checkpointConfig.setCheckpointStorage("file:///C:\\WorkBench\\Work\\flink\\statestore");
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        env.fromSource(new IntSource(), WatermarkStrategy.noWatermarks(), "customized_source")
                .print();

        env.execute();
    }
}
