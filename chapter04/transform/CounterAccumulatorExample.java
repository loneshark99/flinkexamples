package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CounterAccumulatorExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements("apache", "flink", "is", "a", "streaming", "data", "process", "framework")
                .map(new RichMapFunction<String, String>() {

                    private final IntCounter numOfWords = new IntCounter();

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        getRuntimeContext().addAccumulator("howmanywords", numOfWords);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        numOfWords.add(1);
                        return value.toUpperCase();
                    }
                })
                .print();


        JobExecutionResult executionResult = env.execute();
        Integer howmanywords = executionResult.getAccumulatorResult("howmanywords");
        System.out.println("The job executed finished and totally processed " + howmanywords + " words");

    }
}
