package org.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.common.JobBaseCommon;


public class KeyedProcessFunctionDemo_2 {
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        DataStream<Integer> numbers = env.fromData(1, 2, 3, 4, 5, 6);

        numbers.keyBy(n -> n % 2 == 0 ? "even" : "odd")
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
     * Processes keyed integer records by maintaining a running sum per key ("even" or "odd").
     * On each element, it reads the current sum from state, adds the incoming record, updates
     * the state, and emits a string with the key, subtask info, and updated sum.
     */
    private static class CustomKeyedProcessFunction extends KeyedProcessFunction<String, Integer, String> {

        /**
         * Keyed state holding the running sum for the current key.
         * Because this is keyed state, Flink automatically scopes it to the key of the element
         * being processed — "even" and "odd" each maintain their own independent sum value.
         * The single piece of logic written here (read → add → update) therefore runs correctly
         * for every key without any manual key-checking; Flink handles the isolation.
         */
        private transient ValueState<Integer> sumState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Integer> sumDescriptor = new ValueStateDescriptor<>("sum", Integer.class);
            sumState = getRuntimeContext().getState(sumDescriptor);
        }

        @Override
        public void processElement(
                Integer record,
                Context ctx,
                Collector<String> out) throws Exception {

            String key = ctx.getCurrentKey();
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

            Integer currentSum = sumState.value();
            if (currentSum == null) {
                currentSum = 0;
            }
            currentSum += record;
            sumState.update(currentSum);
            out.collect(">> [" + key + "] " + "process: " + record + ", subtask=" + subtaskIndex + "/" + numSubtasks + ", sum=" + currentSum);
        }
    }

}
