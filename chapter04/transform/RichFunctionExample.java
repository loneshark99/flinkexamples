package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//--threshold 3
public class RichFunctionExample {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setGlobalJobParameters(parameterTool);

        env.fromElements(1, 2, 3, 4, 5, 6, 7)
                .filter(new MyFilterFunction())
                .print();
        env.execute();
    }

    private static class MyFilterFunction extends RichFilterFunction<Integer> {

        private int threshold;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("the open method is called");
            ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            this.threshold = parameterTool.getInt("threshold", 5);
        }

        @Override
        public boolean filter(Integer value) throws Exception {
            return value > threshold;
        }

        @Override
        public void close() throws Exception {
            System.out.println("the close method is called");
        }
    }
}
