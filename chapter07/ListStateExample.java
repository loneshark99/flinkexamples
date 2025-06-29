package com.alexwang.flink.chapter07;

import com.alexwang.flink.model.Score;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ListStateExample {
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
                .process(new KeyedProcessFunction<String, Score, Iterable<Score>>() {

                    private ListState<Score> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        this.listState = getRuntimeContext().getListState(new ListStateDescriptor<Score>(
                                "listState", Types.POJO(Score.class)
                        ));
                    }

                    @Override
                    public void processElement(Score score, Context ctx, Collector<Iterable<Score>> out) throws Exception {
                        listState.add(score);
                        List<Score> top5Score = StreamSupport.stream(listState.get().spliterator(), false)
                                .sorted((o1, o2) -> (int) (o2.getScore() - o1.getScore()))
                                .limit(5)
                                .collect(Collectors.toList());
                        this.listState.clear();
                        this.listState.addAll(top5Score);
                        out.collect(top5Score);
                    }
                }).print();
        env.execute();
    }
}
