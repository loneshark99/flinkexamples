package com.alexwang.flink.chapter07;

import com.alexwang.flink.model.Score;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MapStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.socketTextStream("localhost", 4567)
                .map(line -> {
                    String[] fields = line.split(",");
                    return Score.builder().classID(fields[0])
                            .studentID(fields[1]).courseName(fields[2])
                            .score(Float.parseFloat(fields[3])).build();
                }, Types.POJO(Score.class))
                .keyBy(Score::getClassID)
                .process(new KeyedProcessFunction<String, Score, Tuple3<String, String, Float>>() {

                    private MapState<String, Float> mapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.mapState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<String, Float>("mapState", String.class, Float.class)
                        );
                    }

                    @Override
                    public void processElement(Score score, Context ctx, Collector<Tuple3<String, String, Float>> out) throws Exception {
                        Float avgCourseScore = this.mapState.get(score.getCourseName());
                        if (avgCourseScore == null) {
                            mapState.put(score.getCourseName(), score.getScore());
                        } else {
                            mapState.put(score.getCourseName(), (score.getScore() + avgCourseScore) / 2.f);
                        }
                        out.collect(Tuple3.of(ctx.getCurrentKey(), score.getCourseName(), mapState.get(score.getCourseName())));
                    }
                }).print();
        env.execute();
    }
}
