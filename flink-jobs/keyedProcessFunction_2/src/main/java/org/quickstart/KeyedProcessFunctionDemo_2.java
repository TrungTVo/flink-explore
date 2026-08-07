package org.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.common.JobBaseCommon;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;


public class KeyedProcessFunctionDemo_2 {
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        GeneratorFunction<Long, Long> generatorFunction = index -> index + 1;
        DataGeneratorSource<Long> source = new DataGeneratorSource<>(generatorFunction, 1000, Types.LONG);
        DataStream<Long> numbers = env.fromSource(source, WatermarkStrategy.noWatermarks(), "numbers-generator");

        numbers.keyBy(n -> n % 2 == 0 ? "even" : "odd")
            .process(new CustomKeyedProcessFunction())
            .name("sum-by-key")
            .uid("sum-by-key")
            .print("output")
            .name("output-sink")
            .uid("output-sink");

        JobClient jobClient = env.executeAsync("keyedProcessFunction_2 Demo");

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
    private static class CustomKeyedProcessFunction extends KeyedProcessFunction<String, Long, String> {

        /**
         * Keyed state holding the running sum for the current key.
         * Because this is keyed state, Flink automatically scopes it to the key of the element
         * being processed — "even" and "odd" each maintain their own independent sum value.
         * The single piece of logic written here (read → add → update) therefore runs correctly
         * for every key without any manual key-checking; Flink handles the isolation.
         */
        private transient ValueState<Long> sumState;

        @Override
        public void open(OpenContext openContext) throws Exception {
            ValueStateDescriptor<Long> sumDescriptor = new ValueStateDescriptor<>("sum", Long.class);
            sumState = getRuntimeContext().getState(sumDescriptor);
        }

        @Override
        public void processElement(
                Long record,
                Context ctx,
                Collector<String> out) throws Exception {

            String key = ctx.getCurrentKey();
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

            Long currentSum = sumState.value();
            if (currentSum == null) {
                currentSum = 0L;
            }
            currentSum += record;
            sumState.update(currentSum);
            out.collect(">> [" + key + "] " + "process: " + record + ", subtask=" + subtaskIndex + "/" + numSubtasks + ", sum=" + currentSum);
        }
    }

}
