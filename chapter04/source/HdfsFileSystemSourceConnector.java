package com.alexwang.flink.chapter04.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class HdfsFileSystemSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //delete this code line when running at the server or use kerberos for auth
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat()
                , new Path("hdfs://hadoop151:8020/test")).build();

        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "hdfs_file_source")
                .flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
                    Arrays.stream(line.split("\\s+"))
                            .map(w -> Tuple2.of(w, 1L))
                            .forEach(collector::collect);
                }, Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .print();

        env.execute("hdfs file source connector");
    }
}
