package org.quickstart;

import org.apache.flink.util.ParameterTool;

/**
 * Command-line options for selecting the comparison scenario.
 */
final class DemoOptions {

    private final ParameterTool parameters;
    private final boolean failureDemo;
    private final boolean checkpointingEnabled;

    private DemoOptions(
            ParameterTool parameters,
            boolean failureDemo,
            boolean checkpointingEnabled) {
        this.parameters = parameters;
        this.failureDemo = failureDemo;
        this.checkpointingEnabled = checkpointingEnabled;
    }

    static DemoOptions fromArgs(String[] args) {
        ParameterTool parameters = ParameterTool.fromArgs(args);
        return new DemoOptions(
                parameters,
                parameters.getBoolean("failure-demo", false),
                parameters.getBoolean("checkpointing", false));
    }

    ParameterTool parameters() {
        return parameters;
    }

    boolean failureDemo() {
        return failureDemo;
    }

    boolean checkpointingEnabled() {
        return checkpointingEnabled;
    }
}
