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
 * Flink streaming job demonstrating {@link KeyedProcessFunction} with per-key state,
 * routing numeric and alphabetic inputs to separate accumulators.
 */
public class KeyedProcessFunctionDemo {

    /**
     * Builds and submits the Flink pipeline, then waits for the job to finish.
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
         * Initializes the Flink managed state descriptors for sum and concat accumulators.
         */
        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Integer> sumDescriptor = new ValueStateDescriptor<>("sum", Integer.class);
            sumState = getRuntimeContext().getState(sumDescriptor);

            ValueStateDescriptor<String> concatDescriptor = new ValueStateDescriptor<>("concat", String.class);
            concatState = getRuntimeContext().getState(concatDescriptor);
        }

        /**
         * Routes each record to the numeric or alphabetic accumulator based on its key,
         * updates the corresponding state, and emits the updated aggregate.
         *
         * @param record the incoming string record
         * @param ctx    process function context providing the current key
         * @param out    collector for output records
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
