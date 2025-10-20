package org.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class SavepointStateReader {
    public static void main(String[] args) throws Exception {
        // Path to the savepoint you want to analyze
        String savepointPath = "/home/yash/Downloads/savepoint-c7a384-18b7d3e8c9b9";

        // Set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // Load the savepoint
        ExistingSavepoint savepoint = Savepoint.load(env, savepointPath, new MemoryStateBackend());

        // Read the word-count state from the operator
        // Note: You need to know the UID of the operator whose state you want to read
        // If not explicitly set, Flink generates UIDs based on the position in the job graph
        DataSet<Tuple2<String, Integer>> wordCountState = savepoint
                .readKeyedState("stateful-counter", new WordCountStateReader());

        // Print the state
        System.out.println("Word count state from savepoint:");
        wordCountState.print();
    }

    public static class WordCountStateReader extends KeyedStateReaderFunction<String, Tuple2<String, Integer>> {
        private transient ValueState<Integer> countState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<>(
                    "word-count-state",
                    TypeInformation.of(new TypeHint<Integer>() {})
            );
            countState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void readKey(String key, Context context, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer count = countState.value();
            if (count != null) {
                out.collect(new Tuple2<>(key, count));
            }
        }
    }
}
