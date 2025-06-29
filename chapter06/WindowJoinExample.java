package com.alexwang.flink.chapter06;

import com.alexwang.flink.model.Address;
import com.alexwang.flink.model.User;
import com.alexwang.flink.model.UserWithAddress;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowJoinExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<User> userDS = env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new User(Integer.parseInt(fields[0]), fields[1]);
                }, Types.POJO(User.class));

        SingleOutputStreamOperator<Address> addressDS = env.socketTextStream("localhost", 4568)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Address(Integer.parseInt(fields[0]), fields[1]);
                }, Types.POJO(Address.class));

        userDS.join(addressDS)
                .where(User::getId)
                .equalTo(Address::getId)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .apply(new WindowJoinFunction())
                .print();
        env.execute();
    }

    private static class WindowJoinFunction implements JoinFunction<User, Address, UserWithAddress> {

        @Override
        public UserWithAddress join(User first, Address second) throws Exception {
            return UserWithAddress.builder()
                    .user(first)
                    .address(second).build();
        }
    }
}
