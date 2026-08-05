package org.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;
import org.common.models.ElectronicOrder;

/**
 * Runs the lateness and checkpoint recovery demonstration.
 *
 * <p>The supporting classes separate environment configuration, input data,
 * watermark generation, failure injection, operator state, and window
 * aggregation from this entry point.
 */
public class LatenessNoAcceptSideOutputCheckpointDemo {

    public static void main(String[] args) throws Exception {
        DemoOptions options = DemoOptions.fromArgs(args);
        StreamExecutionEnvironment env = DemoEnvironmentFactory.create(options);
        OutputTag<ElectronicOrder> lateTag =
                new OutputTag<ElectronicOrder>("too-late-records") {};

        SingleOutputStreamOperator<Double> windowTotals =
                CheckpointDemoPipeline.build(env, options, lateTag);

        windowTotals.print("output >>>");
        windowTotals.getSideOutput(lateTag).print("late-records >>>");

        JobExecutionResult result =
                env.execute("LatenessNoAcceptSideOutputCheckpointDemo");
        System.out.println("Job finished with runtime: " + result.getNetRuntime() + " ms");
    }
}
