package org.quickstart;

import java.time.Duration;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Creates and configures the Flink execution environment.
 */
final class DemoEnvironmentFactory {

    private static final long CHECKPOINT_INTERVAL_MS = 500L;

    private DemoEnvironmentFactory() {}

    static StreamExecutionEnvironment create(DemoOptions options) {
        Configuration configuration = createConfiguration(options);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);
        env.getConfig().setGlobalJobParameters(options.parameters());

        configureCheckpointing(env, options.checkpointingEnabled());
        printScenario(options);
        return env;
    }

    private static Configuration createConfiguration(DemoOptions options) {
        Configuration configuration = new Configuration();
        if (options.failureDemo()) {
            // Both comparison runs retry once. The checkpoint determines whether
            // that retry starts from zero or from restored state.
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
            configuration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(1));
        }
        return configuration;
    }

    private static void configureCheckpointing(
            StreamExecutionEnvironment env,
            boolean checkpointingEnabled) {
        if (!checkpointingEnabled) {
            System.out.println("Checkpointing: DISABLED");
            return;
        }

        env.enableCheckpointing(
                CHECKPOINT_INTERVAL_MS, CheckpointingMode.EXACTLY_ONCE);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(10_000L);
        checkpointConfig.setMinPauseBetweenCheckpoints(100L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);

        System.out.println(
                "Checkpointing: ENABLED (EXACTLY_ONCE, interval="
                        + CHECKPOINT_INTERVAL_MS
                        + " ms, JobManager checkpoint storage)");
    }

    private static void printScenario(DemoOptions options) {
        System.out.println(
                "Failure demo: " + (options.failureDemo() ? "ENABLED" : "DISABLED"));
    }
}
