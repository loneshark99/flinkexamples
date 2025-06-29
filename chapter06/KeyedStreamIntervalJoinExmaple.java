package com.alexwang.flink.chapter06;

import com.alexwang.flink.model.Address;
import com.alexwang.flink.model.User;
import com.alexwang.flink.model.UserWithAddress;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class KeyedStreamIntervalJoinExmaple {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Tuple3<Integer, String, Long>> userDS = env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] fields = line.split(",");
                    return Tuple3.of(Integer.parseInt(fields[0]), fields[1], Long.parseLong(fields[2]));
                }, Types.TUPLE(Types.INT, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Integer, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(context -> ((element, recordTimestamp) -> element.f2))
                        .withIdleness(Duration.ofSeconds(1)));

        SingleOutputStreamOperator<Tuple3<Integer, String, Long>> addressDS = env.socketTextStream("localhost", 4568)
                .map(line -> {
                    String[] fields = line.split(",");
                    return Tuple3.of(Integer.parseInt(fields[0]), fields[1], Long.parseLong(fields[2]));
                }, Types.TUPLE(Types.INT, Types.STRING, Types.LONG))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Integer, String, Long>>forMonotonousTimestamps()
                        .withTimestampAssigner(context -> ((element, recordTimestamp) -> element.f2))
                        .withIdleness(Duration.ofSeconds(1)));

        userDS.keyBy(t -> t.f0)
                .intervalJoin(addressDS.keyBy(t -> t.f0))
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new MyProcessJoinFunction())
                .print();
        env.execute();
    }

    private static class MyProcessJoinFunction extends ProcessJoinFunction<Tuple3<Integer, String, Long>, Tuple3<Integer, String, Long>, UserWithAddress> {

        @Override
        public void processElement(Tuple3<Integer, String, Long> left, Tuple3<Integer, String, Long> right, Context ctx, Collector<UserWithAddress> out) throws Exception {
            out.collect(UserWithAddress.builder()
                    .user(User.builder().name(left.f1).id(left.f0).build())
                    .address(Address.builder().id(right.f0).from(right.f1).build())
                    .build());
        }
    }
}
