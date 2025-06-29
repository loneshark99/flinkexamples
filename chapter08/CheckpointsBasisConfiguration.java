package com.alexwang.flink.chapter08;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CheckpointsBasisConfiguration {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(2_000L, CheckpointingMode.EXACTLY_ONCE);

        final CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("file:///C:\\WorkBench\\Work\\flink\\statestore");
        checkpointConfig.setCheckpointTimeout(10_000L);
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        checkpointConfig.setMinPauseBetweenCheckpoints(1000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.enableUnalignedCheckpoints(true);

        env.setParallelism(2);
        env.socketTextStream("localhost", 4567)
                .map(Integer::parseInt, Types.INT)
                .keyBy(e -> e % 2 == 0)
                .sum(0)
                .print();
        env.execute("checkpoint basis configuration");
    }
}
