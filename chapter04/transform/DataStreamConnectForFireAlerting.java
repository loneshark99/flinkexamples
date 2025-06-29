package com.alexwang.flink.chapter04.transform;

import com.alexwang.flink.model.ForestMonitorData;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import static com.alexwang.flink.model.ForestMonitorData.*;

public class DataStreamConnectForFireAlerting {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<Double> temperatureDS = env.socketTextStream("hadoop151", 4567)
                .map(Double::parseDouble).returns(Types.DOUBLE);

        SingleOutputStreamOperator<String> smokeDs = env.socketTextStream("hadoop151", 4568)
                .map(String::toUpperCase).returns(Types.STRING);

        temperatureDS.connect(smokeDs).map(new CoMapFunction<Double, String, ForestMonitorData>() {

                    @Override
                    public ForestMonitorData map1(Double value) throws Exception {
                        return ForestMonitorData.builder().type(TEMPERATURE).temperature(value).build();
                    }

                    @Override
                    public ForestMonitorData map2(String value) throws Exception {
                        return ForestMonitorData.builder().type(SMOKE).smoke(value).build();
                    }
                })
                .filter(data -> (data.getType().equals(TEMPERATURE) && data.getTemperature() > 45d)
                        || (data.getType().equals(SMOKE) && SMOKE_WARNING.contains(data.getSmoke())))
                .print("fire alerting");

        env.execute("data stream connect");
    }
}
