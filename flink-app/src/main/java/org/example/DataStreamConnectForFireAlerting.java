package org.example;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import static org.example.ForestMonitorData.SMOKE;
import static org.example.ForestMonitorData.TEMPERATURE;


public class DataStreamConnectForFireAlerting {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8081); // Use 0 for dynamic port assignment
        MiniClusterConfiguration clusterConfig = new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(4)
                .setNumSlotsPerTaskManager(2)
                .build();

        MiniCluster miniCluster = new MiniCluster(clusterConfig);
        miniCluster.start();
        int restPort = miniCluster.getRestAddress().get().getPort();
        System.out.println("WebUI is available at http://localhost:" + restPort + "/#/overview");

        Configuration envConfig = new Configuration();
        envConfig.set(RestOptions.ADDRESS, "localhost");
        envConfig.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                "localhost", restPort, envConfig);

        // Add logic here....
        SingleOutputStreamOperator<Double> temperatureStream = env.socketTextStream("localhost", 4568).map(Double::parseDouble).returns(Types.DOUBLE);
        SingleOutputStreamOperator<String> smokeStream =  env.socketTextStream("localhost", 4569).map(String::toUpperCase).returns(Types.STRING);

        SingleOutputStreamOperator<ForestMonitorData> k = temperatureStream.connect(smokeStream).map(new CoMapFunction<Double, String, ForestMonitorData>() {
            @Override
            public ForestMonitorData map1(Double aDouble) throws Exception {
                ForestMonitorData x = new ForestMonitorData();
                x.setType(TEMPERATURE);
                x.setTemperature(aDouble);
                return x;
            }

            @Override
            public ForestMonitorData map2(String s) throws Exception {
                ForestMonitorData x = new ForestMonitorData();
                x.setType(SMOKE);
                x.setSmoke(s);
                return x;
            }
        });

        k.print();



        JobExecutionResult executionResult = env.execute("custom source connector");
        System.out.println("Job finished execution, but the cluster is still running.");
        System.out.println("Press ENTER to terminate the program");
        System.in.read();
        miniCluster.close();
    }
}
