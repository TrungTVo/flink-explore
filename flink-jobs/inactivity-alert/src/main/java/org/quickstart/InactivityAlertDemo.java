package org.quickstart;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


/**
 * Flink streaming job that detects user inactivity by registering a processing-time timer
 * for each keyed event and firing an alert when no new event arrives within the timeout window.
 */
public class InactivityAlertDemo {

    /**
     * Builds the Flink pipeline reading from a socket, keys events by user ID,
     * applies the inactivity detection process function, and submits the job asynchronously.
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> wordCountStream = env.socketTextStream("taskmanager", 9999)
                .map(new Splitter(), Types.TUPLE(Types.STRING, Types.STRING))
                .keyBy((Tuple2<String, String> pair) -> pair.f0, Types.STRING)
                .process(new InactivityAlertProcess());

        wordCountStream.print("output");

        JobClient jobClient = env.executeAsync("InactivityAlertDemo");

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
     * Parses a raw comma-separated input record into a (userId, message) pair.
     */
    private static class Splitter implements MapFunction<String, Tuple2<String, String>> {
        /**
         * Splits the input record on {@code ','} and returns the first two fields as a tuple.
         *
         * @param inputRecord raw CSV line from the socket stream
         * @return tuple of (userId, message)
         */
        @Override
        public Tuple2<String, String> map(String inputRecord) throws Exception {
            String[] recordData = inputRecord.split(",");
            return new Tuple2<>(recordData[0], recordData[1]);
        }
    }

    /**
     * Keyed process function that tracks per-user activity and emits an inactivity alert
     * when no event is received within {@code TIMEOUT_INTERVAL} milliseconds.
     */
    public static class InactivityAlertProcess
            extends KeyedProcessFunction<String, Tuple2<String, String>, String> {

        private static final long TIMEOUT_INTERVAL = 10_000L; // 10 seconds
        private ValueState<Long> lastTimerState;

        /**
         * Initializes the {@code lastTimerState} value state used to track the most recently
         * registered processing-time timer for each user key.
         */
        @Override
        public void open(OpenContext openContext) {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("last-timer", Long.class);
            lastTimerState = getRuntimeContext().getState(descriptor);
        }

        /**
         * Cancels the previous inactivity timer (if any), registers a new one
         * {@code TIMEOUT_INTERVAL} ms in the future, and emits a receipt message.
         *
         * @param value incoming (userId, message) tuple
         * @param ctx   process function context providing the timer service
         * @param out   collector for output strings
         */
        @Override
        public void processElement(
                Tuple2<String, String> value,
                Context ctx,
                Collector<String> out) throws Exception {

            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            long newTimer = currentProcessingTime + TIMEOUT_INTERVAL;
            LocalDateTime currentProcessingTimeLocal = Instant.ofEpochMilli(currentProcessingTime).atZone(ZoneId.systemDefault()).toLocalDateTime();

            Long oldTimer = lastTimerState.value();
            if (oldTimer != null) {
                ctx.timerService().deleteProcessingTimeTimer(oldTimer);
            }

            ctx.timerService().registerProcessingTimeTimer(newTimer);
            lastTimerState.update(newTimer);

            out.collect(currentProcessingTimeLocal + " - Received event for user: " + value.f0 + ", message: " + value.f1);
        }

        /**
         * Fires when no event has been received for the current key within the timeout window,
         * emitting an inactivity alert and clearing the timer state.
         *
         * @param timestamp the processing time at which the timer fired
         * @param ctx       on-timer context providing the current key
         * @param out       collector for the alert string
         */
        @Override
        public void onTimer(
                long timestamp,
                OnTimerContext ctx,
                Collector<String> out) throws Exception {

            LocalDateTime currentTimeLocal = Instant.ofEpochMilli(timestamp).atZone(ZoneId.systemDefault()).toLocalDateTime();
            out.collect(currentTimeLocal + " - User: " + ctx.getCurrentKey() + " inactive for " + TIMEOUT_INTERVAL / 1000 + " seconds");
            lastTimerState.clear();
        }
    }

}
