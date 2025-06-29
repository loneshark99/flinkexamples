package com.alexwang.flink.chapter04.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.PrintStream;
import java.net.Socket;

public class CustomSinkLegacyAPIExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(20_000, CheckpointingMode.AT_LEAST_ONCE);
        GeneratorFunction<Long, String> function = (Long num) -> String.format("The generated text line and Num:%s", num);

        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(
                function,
                1_000L,
                RateLimiterStrategy.perSecond(10),
                Types.STRING
        );
        env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "source")
                .addSink(CustomSink.of("localhost", 3456));
        env.execute();
    }

    private static class CustomSink extends RichSinkFunction<String> {

        private final String hostname;
        private final int port;
        private Socket socket;
        private PrintStream stream;

        private CustomSink(String hostname, int port) {
            this.hostname = hostname;
            this.port = port;
        }

        public static CustomSink of(String hostname, int port) {
            return new CustomSink(hostname, port);
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.socket = new Socket(hostname, port);
            this.stream = new PrintStream(socket.getOutputStream());
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            this.stream.println(value);
        }

        @Override
        public void close() throws Exception {
            if (this.stream != null)
                this.stream.close();
            if (this.socket != null && !this.socket.isClosed())
                this.socket.close();
        }
    }
}
