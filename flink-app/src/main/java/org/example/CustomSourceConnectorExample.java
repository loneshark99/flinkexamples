package org.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomSourceConnectorExample {
    public static void main(String[] args) throws Exception {
        // Create configuration with dynamic port assignment
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 0); // Use 0 for dynamic port assignment

        // Create and start mini cluster
        MiniClusterConfiguration clusterConfig = new MiniClusterConfiguration.Builder()
                .setConfiguration(config)
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(2)
                .build();

        MiniCluster miniCluster = new MiniCluster(clusterConfig);
        miniCluster.start();

        // Get the actual port that was assigned
        int restPort = miniCluster.getRestAddress().get().getPort();
        System.out.println("WebUI is available at http://localhost:" + restPort + "/#/overview");

        // Create StreamExecutionEnvironment that connects to the mini cluster
        Configuration envConfig = new Configuration();
        envConfig.set(RestOptions.ADDRESS, "localhost");
        envConfig.set(RestOptions.PORT, restPort);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                "localhost", restPort, envConfig);
        env.addSource(new SimpleSourceFunction(), "simpleSource")
                .print();

        // Execute job
        env.execute("custom source connector");

        System.out.println("Job finished execution, but the cluster is still running");
        System.out.println("Press ENTER to terminate the program");
        System.in.read();

        // Clean up when done
        miniCluster.close();
    }
}
