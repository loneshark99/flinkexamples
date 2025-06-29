package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class KeyedStreamRollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        List<Tuple3<Integer, Integer, Integer>> data = new ArrayList<>();
        data.add(new Tuple3<>(0, 2, 2));
        data.add(new Tuple3<>(0, 1, 1));
        data.add(new Tuple3<>(0, 5, 6));
        data.add(new Tuple3<>(0, 3, 5));

        data.add(new Tuple3<>(1, 1, 9));
        data.add(new Tuple3<>(1, 2, 8));
        data.add(new Tuple3<>(1, 3, 10));
        data.add(new Tuple3<>(1, 2, 9));
        data.add(new Tuple3<>(1, 20, 7));

        env.fromCollection(data)
                .keyBy(t -> t.f0)
                //.sum(2)
                //.min(2)
                //.max(2)
                //.minBy(2)
                .maxBy(2)
                .print();

        env.execute("rolling aggregation");
    }
}
