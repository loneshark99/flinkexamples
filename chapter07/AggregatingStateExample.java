package com.alexwang.flink.chapter07;

import com.alexwang.flink.model.CityTemperature;
import com.alexwang.flink.model.TemperatureStats;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class AggregatingStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] values = line.split(",");
                    return CityTemperature.builder()
                            .city(values[0])
                            .temperature(Float.parseFloat(values[1]))
                            .timestamp(Long.parseLong(values[2]))
                            .build();

                }, Types.POJO(CityTemperature.class))
                .keyBy(CityTemperature::getCity)
                .process(new KeyedProcessFunction<String, CityTemperature, TemperatureStats>() {

                    private AggregatingState<CityTemperature, TemperatureStats> aggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.aggregatingState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<CityTemperature, TemperatureStats, TemperatureStats>(
                                        "aggregatingState",
                                        new TemperatureStatsAggregateFunction(),
                                        Types.POJO(TemperatureStats.class)
                                )
                        );
                    }

                    @Override
                    public void processElement(CityTemperature value, Context ctx, Collector<TemperatureStats> out)
                            throws Exception {
                        this.aggregatingState.add(value);
                        out.collect(this.aggregatingState.get());
                    }
                })
                .print();
        env.execute();
    }

    private static class TemperatureStatsAggregateFunction implements AggregateFunction<CityTemperature, TemperatureStats, TemperatureStats> {

        @Override
        public TemperatureStats createAccumulator() {
            System.out.println("the createAccumulator method is called");
            return TemperatureStats.builder()
                    .average(Float.MIN_VALUE)
                    .min(Float.MAX_VALUE)
                    .max(Float.MIN_VALUE)
                    .build();
        }

        @Override
        public TemperatureStats add(CityTemperature value, TemperatureStats accumulator) {
            System.out.println("the add method is called, newValue:" + value + ",accumulator:" + accumulator);
            TemperatureStats temperatureStats = new TemperatureStats();
            temperatureStats.setCity(value.getCity());
            temperatureStats.setMin(Math.min(accumulator.getMin(), value.getTemperature()));
            temperatureStats.setMax(Math.max(accumulator.getMax(), value.getTemperature()));
            temperatureStats.setAverage((accumulator.getAverage() == Float.MIN_VALUE)
                    ? value.getTemperature() : (accumulator.getAverage() + value.getTemperature()) / 2);
            return temperatureStats;
        }

        @Override
        public TemperatureStats getResult(TemperatureStats accumulator) {
            return TemperatureStats.build(accumulator);
        }

        @Override
        public TemperatureStats merge(TemperatureStats a, TemperatureStats b) {

            return TemperatureStats.builder()
                    .city(a.getCity())
                    .min(Math.min(a.getMin(), b.getMin()))
                    .max(Math.max(a.getMax(), b.getMax()))
                    .average((a.getAverage() + b.getAverage()) / 2)
                    .build();
        }
    }
}
