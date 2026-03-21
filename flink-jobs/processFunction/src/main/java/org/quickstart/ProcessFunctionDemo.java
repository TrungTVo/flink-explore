package org.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.common.JobBaseCommon;

public class ProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        DataStream<String> input = env.fromData("100", "abc", "200", "xyz", "300");

        OutputTag<String> invalidTag = new OutputTag<String>("invalid-records") {
        };

        SingleOutputStreamOperator<Integer> validNumbers = input.process(new FilterProcessFunction(invalidTag)).returns(Types.INT);

        DataStream<String> invalidRecords = validNumbers.getSideOutput(invalidTag);

        validNumbers.print("VALID");
        invalidRecords.print("INVALID");

        JobClient jobClient = env.executeAsync("processFunctionDemo");

        System.out.println("Job submitted, waiting for completion...");

        // Get the final result
        try {
            JobExecutionResult result = jobClient.getJobExecutionResult().get();
            System.out.println("Job finished with runtime: " + result.getNetRuntime() + " ms");
        } catch (Exception e) {
            System.err.println("Failed to get job execution result: " + e.getMessage());
        }
    }

    /**
     * A custom ProcessFunction that parses strings into integers. 
     * Valid integers are emitted to the main output, while invalid records are sent to a side output.
     */
    private static class FilterProcessFunction extends ProcessFunction<String, Integer> {
        private OutputTag<String> invalidTag;

        FilterProcessFunction(OutputTag<String> invalidTag) {
            this.invalidTag = invalidTag;
        }

        @Override
        public void processElement(
                String value,
                Context ctx,
                Collector<Integer> out) {

            try {
                int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
                System.out.println("process: " + value + ", subtask=" + subtaskIndex + "/" + numSubtasks);
                int number = Integer.parseInt(value);
                out.collect(number);                    // main output
            } catch (NumberFormatException e) {
                ctx.output(this.invalidTag, value);     // side output
            }
        }
    }

}
