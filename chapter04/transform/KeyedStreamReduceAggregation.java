package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class KeyedStreamReduceAggregation {
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

        DataStreamSource<Tuple3<Integer, Integer, Integer>> items = env.fromCollection(data);
        items.keyBy(t -> t.f0)
                .reduce((ReduceFunction<Tuple3<Integer, Integer, Integer>>) (aggValue, newValue) -> {
                    //min()
                    //return Tuple3.of(aggValue.f0, aggValue.f1, Math.min(aggValue.f2, newValue.f2));
                    //minBy()
                    return Tuple3.of(aggValue.f0,
                            aggValue.f2 < newValue.f2 ? aggValue.f1 : newValue.f1,
                            Math.min(aggValue.f2, newValue.f2));

                })
                .print();


        env.execute("rolling aggregation");
    }
}
