package com.alexwang.flink.chapter04.source;

import com.alexwang.flink.model.Person;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvReaderFormat;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LocalFileSystemSourceConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        /*FileSource<String> fileSource = FileSource.<String>forRecordStreamFormat(new TextLineInputFormat(), new Path("input"))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();*/

        /*FileSource<String> fileSource = FileSource.<String>forRecordStreamFormat(new TextLineInputFormat(), new Path("input"))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();*/

        /*CsvReaderFormat<Person> csvReaderFormat = CsvReaderFormat.forPojo(Person.class);
        FileSource<Person> fileSource = FileSource.forRecordStreamFormat(csvReaderFormat, new Path("csv"))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();*/

        CsvReaderFormat<Person> csvReaderFormat = CsvReaderFormat.forPojo(Person.class);

        FileSource<Person> fileSource = FileSource.forBulkFileFormat(
                        new StreamFormatAdapter<>(csvReaderFormat), new Path("csv"))
                .monitorContinuously(Duration.ofSeconds(10))
                .build();
        env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "file_source")
                .print();

        env.execute("file source connector");
    }
}
