package org.example;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class CounterAccumulatorExample {
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

        env.fromElements("apache", "flink", "is", "a", "streaming", "data", "process", "framework",
                        "apache", "flink", "is", "a", "streaming", "data", "process", "framework",
                        "apache", "flink", "is", "a", "streaming", "data", "process", "framework",
                        "apache", "flink", "is", "a", "streaming", "data", "process", "framework",
                        "apache", "flink", "is", "a", "streaming", "data", "process", "framework",
                        "apache", "flink", "is", "a", "streaming", "data", "process", "framework",
                        "apache", "flink", "is", "a", "streaming", "data", "process", "framework")
                .map(new RichMapFunction<String, String>() {

                    private transient IntCounter counter;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        counter = new org.apache.flink.api.common.accumulators.IntCounter();
                        getRuntimeContext().addAccumulator("word-counter", counter);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        counter.add(1);
                        return value.toUpperCase();
                    }

                })
                .print();




        JobExecutionResult executionResult = env.execute("custom source connector");
        int wordCount = executionResult.getAccumulatorResult("word-counter");
        System.out.println("Total number of words processed: " + wordCount);
        System.out.println("Job finished execution, but the cluster is still running.");
        System.out.println("Press ENTER to terminate the program");
        System.in.read();
        miniCluster.close();
    }
}
