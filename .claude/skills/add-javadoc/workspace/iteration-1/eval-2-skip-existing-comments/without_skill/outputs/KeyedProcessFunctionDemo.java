package org.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.common.JobBaseCommon;
import org.quickstart.interfaces.CustomFormat;
import org.quickstart.models.CustomLetter;
import org.quickstart.models.CustomNumber;

/**
 * Demonstrates the use of Flink's {@link KeyedProcessFunction} for processing
 * keyed streams with per-key state management.
 *
 * <p>This class sets up a Flink streaming job that processes a stream of string
 * values, routing them to different state accumulators based on whether they
 * are numeric or alphabetic.
 */
public class KeyedProcessFunctionDemo {

    /**
     * The entry point for the KeyedProcessFunction demo Flink job.
     *
     * <p>Configures the streaming environment, creates an input stream with
     * sample data, applies key-based routing and stateful processing, and
     * waits for the job to complete.
     *
     * @param args command-line arguments (not used)
     * @throws Exception if the job fails to execute
     */
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        DataStream<String> input = env.fromData("100", "abc", "200", "xyz", "300");

        input.keyBy(new CustomKeySelector(), Types.STRING)
            .process(new CustomKeyedProcessFunction())
            .print("output");

        JobClient jobClient = env.executeAsync("keyedProcessFunction Demo");

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
     * Routes each input record to a key group based on whether the value is numeric or alphabetic.
     * Numeric strings are keyed as "number"; all others are keyed as "letter".
     */
    private static class CustomKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String inputRecord) {
            try {
                Integer.parseInt(inputRecord);
                return "number";
            } catch (NumberFormatException e) {
                return "letter";
            }
        }
    }


    /**
     * Processes keyed records by maintaining per-key state:
     * - "number" keys accumulate a running integer sum via {@code sumState}.
     * - "letter" keys build a concatenated uppercase string via {@code concatState}.
     * Emits a {@link CustomNumber} or {@link CustomLetter} on each processed element.
     */
    private static class CustomKeyedProcessFunction extends KeyedProcessFunction<String, String, CustomFormat> {

        /** Tracks the running total of all numeric inputs seen so far for the "number" key. Initialized to null by Flink; treated as 0 on first access. */
        private transient ValueState<Integer> sumState;

        /** Builds a comma-separated, uppercased string of all non-numeric inputs for the "letter" key. Initialized to null by Flink; treated as "" on first access. */
        private transient ValueState<String> concatState;

        /**
         * Initializes the Flink state descriptors for {@code sumState} and {@code concatState}.
         * Called by Flink before the first element is processed.
         *
         * @param openContext context provided by Flink during operator initialization
         * @throws Exception if state initialization fails
         */
        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Integer> sumDescriptor = new ValueStateDescriptor<>("sum", Integer.class);
            sumState = getRuntimeContext().getState(sumDescriptor);

            ValueStateDescriptor<String> concatDescriptor = new ValueStateDescriptor<>("concat", String.class);
            concatState = getRuntimeContext().getState(concatDescriptor);
        }

        /**
         * Processes each element in the keyed stream by updating the appropriate state
         * and emitting a result record.
         *
         * <p>For numeric keys, adds the parsed integer to the running sum. For letter keys,
         * appends the uppercased value to the concatenated string.
         *
         * @param record the current input record
         * @param ctx    the process function context providing access to the current key and timers
         * @param out    the collector used to emit output records
         * @throws Exception if state access or output collection fails
         */
        @Override
        public void processElement(
                String record,
                Context ctx,
                Collector<CustomFormat> out) throws Exception {

            String key = ctx.getCurrentKey();
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();
            System.out.println(">> [" + key + "] " + "process: " + record + ", subtask=" + subtaskIndex + "/" + numSubtasks);
            if (key.equals("number")) {
                Integer currentSum = sumState.value();
                if (currentSum == null) {
                    currentSum = 0;
                }
                currentSum += Integer.parseInt(record);
                sumState.update(currentSum);
                out.collect(new CustomNumber(currentSum));
            } else {
                String currentConcat = concatState.value();
                if (currentConcat == null) {
                    currentConcat = "";
                }
                currentConcat += record.toUpperCase() + ", ";
                concatState.update(currentConcat);
                out.collect(new CustomLetter(currentConcat));
            }
        }
    }

}
