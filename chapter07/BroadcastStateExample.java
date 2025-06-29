package com.alexwang.flink.chapter07;

import com.alexwang.flink.model.CityTemperature;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<CityTemperature> temperatureDS = env.socketTextStream("localhost", 4567).map(line -> {
                    String[] values = line.split(",");
                    return CityTemperature.builder().city(values[0]).temperature(Float.parseFloat(values[1]))
                            .timestamp(Long.parseLong(values[2])).build();
                }, Types.POJO(CityTemperature.class))
                .keyBy(CityTemperature::getCity)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .reduce((aggregateValue, newValue) -> {
                    float averageTemperature = (aggregateValue.getTemperature() + newValue.getTemperature()) / 2;
                    return CityTemperature.builder().city(aggregateValue.getCity())
                            .temperature(averageTemperature)
                            .timestamp(aggregateValue.getTimestamp())
                            .build();
                });
        SingleOutputStreamOperator<Float> thresholdDS = env.socketTextStream("localhost", 4568).map(Float::parseFloat);

        final MapStateDescriptor<String, Float> mapStateDescriptor = new MapStateDescriptor<>("threshold", String.class, Float.class);
        BroadcastStream<Float> broadcastStream = thresholdDS.broadcast(mapStateDescriptor);
        temperatureDS.connect(broadcastStream)
                .process(new BroadcastProcessFunction<CityTemperature, Float, String>() {
                    @Override
                    public void processElement(CityTemperature value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        ReadOnlyBroadcastState<String, Float> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        Float threshold = broadcastState.get("threshold");
                        if (threshold == null)
                            return;
                        if (value.getTemperature() >= threshold) {
                            out.collect(String.format("The city %s average temperature is %s in past one minute, exceed threshold %s", value.getCity(),
                                    value.getTemperature(), threshold));
                        }
                    }

                    @Override
                    public void processBroadcastElement(Float value, Context ctx, Collector<String> out) throws Exception {
                        BroadcastState<String, Float> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        broadcastState.put("threshold", value);
                    }
                }).print();
        env.execute("broadcast example");
    }
}
