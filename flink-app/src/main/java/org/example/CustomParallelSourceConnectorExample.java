package org.example;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CustomParallelSourceConnectorExample {
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 0); // Use 0 for dynamic port assignment
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
        env.addSource(new SimpleParallelSourceFunction(10, 1000), "simpleSource")
                .setParallelism(4)
                .slotSharingGroup("source-group")
                .rebalance()
                .print()
                .setParallelism(4)
                .slotSharingGroup("sink-group");

        env.execute("custom source connector");
        System.out.println("Job finished execution, but the cluster is still running");
        System.out.println("Press ENTER to terminate the program");
        System.in.read();
        miniCluster.close();
    }
}
