package com.alexwang.flink.chapter04.transform;

import com.alexwang.flink.model.Address;
import com.alexwang.flink.model.User;
import com.alexwang.flink.model.UserWithAddress;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

public class DataStreamInnerJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<User> userDs = env.socketTextStream("hadoop151", 4567).map(line -> {
            String[] fields = line.split(",");
            return new User(Integer.parseInt(fields[0]), fields[1]);
        }, PojoTypeInfo.of(User.class));

        SingleOutputStreamOperator<Address> addressDs = env.socketTextStream("hadoop151", 4568).map(line -> {
            String[] fields = line.split(",");
            return new Address(Integer.parseInt(fields[0]), fields[1]);
        }, Types.POJO(Address.class));

        userDs.connect(addressDs).keyBy(User::getId, Address::getId)
                .process(new KeyedCoProcessFunction<Integer, User, Address, UserWithAddress>() {

                    private MapState<Integer, User> left;
                    private MapState<Integer, Address> right;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.left = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Integer, User>("leftMap", Types.INT, Types.POJO(User.class))
                        );
                        this.right = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Integer, Address>("rightMap", Types.INT, Types.POJO(Address.class))
                        );
                    }

                    @Override
                    public void processElement1(User value, Context ctx, Collector<UserWithAddress> out) throws Exception {
                        this.left.put(value.getId(), value);
                        Address address = this.right.get(value.getId());
                        if (address != null) {
                            out.collect(UserWithAddress.builder().user(value).address(address).build());
                        }
                    }

                    @Override
                    public void processElement2(Address value, Context ctx, Collector<UserWithAddress> out) throws Exception {
                        this.right.put(value.getId(), value);
                        User user = this.left.get(value.getId());
                        if (user != null) {
                            out.collect(UserWithAddress.builder().user(user).address(value).build());
                        }
                    }
                }).map(uwa -> String.format("The user %s come from %s (id=%d)", uwa.getUser().getName(), uwa.getAddress().getFrom(), uwa.getUser().getId()))
                .print();

        env.execute("datastream connect");
    }
}
