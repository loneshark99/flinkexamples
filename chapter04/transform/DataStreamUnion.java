package com.alexwang.flink.chapter04.transform;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamUnion {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> ds2 = env.fromElements(5, 6, 7, 8);
        DataStreamSource<String> ds3 = env.fromElements("9", "10");
        ds1.union(ds2, ds3.map(Integer::parseInt, Types.INT))
                .print();

        env.execute("data stream union");
    }
}
