package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.rocksdb.Checkpoint;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StreamingJobWithState {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EmbeddedRocksDBStateBackend rocksDBStateBackend = new EmbeddedRocksDBStateBackend();

        env.setStateBackend(rocksDBStateBackend);
        env.enableCheckpointing(10000);
        String checkpointDir = "file:///tmp/flink-checkpoints";
        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // Create a simulated word stream source
        DataStream<String> textStream = env.addSource(new WordSource());

        // Process the stream with a stateful operator
        DataStream<Tuple2<String, Integer>> counts = textStream
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .flatMap(new StateFullCounter())
                .uid("stateful-counter");

        // Print the results
        counts.print();

        // Execute the program
        env.execute("Word Count with RocksDB State Backend");
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] tokens = s.toLowerCase().split("\\W+");
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    public static class StateFullCounter extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                    "word-count-state",
                    Integer.class
            );

            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> collector) throws Exception {

            Integer currentCount = countState.value();
            if (currentCount == null) {
                currentCount = 0;
            }

            // Update the count
            int updatedCount = currentCount + value.f1;
            countState.update(updatedCount);

            // Emit the updated count
            collector.collect(new Tuple2<>(value.f0, updatedCount));
        }
    }

    public static class WordSource implements SourceFunction<String> {

        private volatile boolean isRunning = true;
        private final Random random = new Random();
        private final String[] words =  new String[] {
                "apache", "flink", "stream", "processing", "state", "rocksdb", "savepoint", "checkpoint", "exactly", "once", "semantics"
        };

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while(isRunning) {
                synchronized (sourceContext.getCheckpointLock()) {
                    StringBuilder sb = new StringBuilder();
                    int numWords = random.nextInt(3) + 1;
                    for (int i = 0; i < numWords; i++) {
                        if (i > 0) {
                            sb.append(" ");
                        }
                        sb.append(words[random.nextInt(words.length)]);
                    }
                    sourceContext.collect(sb.toString());
                }

                TimeUnit.MILLISECONDS.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
