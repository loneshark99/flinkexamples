package com.alexwang.flink.chapter07;

import com.alexwang.flink.model.Address;
import com.alexwang.flink.model.User;
import com.alexwang.flink.model.UserWithAddress;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class StateTTLExmaple {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<User> userDs = env.socketTextStream("localhost", 4567).map(line -> {
            String[] fields = line.split(",");
            return new User(Integer.parseInt(fields[0]), fields[1]);
        }, PojoTypeInfo.of(User.class));

        SingleOutputStreamOperator<Address> addressDs = env.socketTextStream("localhost", 4568).map(line -> {
            String[] fields = line.split(",");
            return new Address(Integer.parseInt(fields[0]), fields[1]);
        }, Types.POJO(Address.class));


        userDs.keyBy(User::getId)
                .connect(addressDs.keyBy(Address::getId))
                .process(new LeftJoinFunction())
                .print();

        env.execute("datastream left join");
    }

    private static class LeftJoinFunction extends KeyedCoProcessFunction<Integer, User, Address, UserWithAddress> {

        private MapState<Integer, Address> mapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            final StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .neverReturnExpired()
                    .build();
            MapStateDescriptor<Integer, Address> descriptor = new MapStateDescriptor<>("mapState", Types.INT, Types.POJO(Address.class));
            descriptor.enableTimeToLive(ttlConfig);
            this.mapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement1(User value, Context ctx, Collector<UserWithAddress> out) throws Exception {
            Address address = this.mapState.get(value.getId());
            out.collect(UserWithAddress.builder()
                    .user(value)
                    .address(address)
                    .build());
        }

        @Override
        public void processElement2(Address value, Context ctx, Collector<UserWithAddress> out) throws Exception {
            this.mapState.put(value.getId(), value);
        }
    }
}
