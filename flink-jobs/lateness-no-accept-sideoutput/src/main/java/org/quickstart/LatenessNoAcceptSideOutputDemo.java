package org.quickstart;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.common.models.ElectronicOrder;

/**
 * Flink streaming demo showing that late records can be silently dropped when the
 * watermark advances too aggressively, even when {@code allowedLateness} is configured.
 *
 * <p>The pipeline uses a custom per-event watermark generator that immediately advances
 * the watermark to the highest seen event timestamp. Combined with a 10-second
 * {@link TumblingEventTimeWindows} and 5 seconds of {@code allowedLateness}, the window
 * [00:00:00, 00:00:10) remains open until the watermark exceeds 00:00:15 — but because
 * record "555" (event time 00:00:18) is processed before the late record "333" (event
 * time 00:00:08), the watermark reaches 00:00:18 and closes the grace period, causing
 * record "333" to be silently dropped.
 */
public class LatenessNoAcceptSideOutputDemo {

    /**
     * Builds and submits the Flink pipeline demonstrating late-record dropping.
     *
     * <p><b>WatermarkStrategy</b> — a custom {@link WatermarkGenerator} emits a new watermark
     * on every event, immediately advancing it to {@code max(seen event timestamps)}.
     * Unlike the built-in {@code forBoundedOutOfOrderness} which emits watermarks periodically
     * on a timer, this eager strategy advances the watermark as fast as events arrive, leaving
     * no built-in tolerance for out-of-order records beyond what {@code allowedLateness} provides.
     *
     * <p><b>TumblingEventTimeWindows(10 s) + allowedLateness(5 s)</b> — window
     * [00:00:00, 00:00:10) fires once the watermark crosses 00:00:10. With 5 seconds of
     * allowed lateness the window state is retained and the window is re-triggered for each
     * late record that arrives while the watermark is still below 00:00:15 (window end +
     * lateness). Records arriving after the watermark exceeds 00:00:15 are silently dropped
     * because no side-output tag is registered. In this demo the eager watermark reaches
     * 00:00:18 before the late record "333" (event time 00:00:08) is processed, so the
     * grace period has already expired and the record is discarded.
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        String start = "2026-01-01T00:00:00Z";

        DataStream<ElectronicOrder> input = env.fromData(
            new ElectronicOrder("111", "trung", "HDTV", 2000.00, 
                Instant.parse(start).toEpochMilli()),                                                           // event time 00:00:00
            
            new ElectronicOrder("222", "aiko", "HDTV", 1999.23, 
                Instant.parse(start).plusSeconds(4).toEpochMilli()),                              // event time 00:00:04
            
            new ElectronicOrder("444", "aiko", "ABCD", 1333.98, 
                Instant.parse(start).plusSeconds(12).toEpochMilli()),                             // event time 00:00:12
            
            new ElectronicOrder("555", "trung", "HDTV", 5000.98, 
                Instant.parse(start).plusSeconds(18).toEpochMilli()),                             // event time 00:00:18
            
            // This record belongs to the first window, but arrives late.
            new ElectronicOrder("333", "trung", "ABCD", 4500.00, 
                Instant.parse(start).plusSeconds(8).toEpochMilli())                               // event time 00:00:08
        );

        OutputTag<ElectronicOrder> lateTag = new OutputTag<ElectronicOrder>("too-late-records") {};

        /** process based on event time */
        SingleOutputStreamOperator<Double> outputWindowedStream = input.assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<ElectronicOrder>forGenerator(ctx -> new WatermarkGenerator<ElectronicOrder>() {
                        private long maxTimestamp = Long.MIN_VALUE;

                        @Override
                        public void onEvent(ElectronicOrder event, long eventTimestamp, WatermarkOutput output) {
                            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                            output.emitWatermark(new Watermark(maxTimestamp));
                        }

                        @Override
                        public void onPeriodicEmit(WatermarkOutput output) {
                            // This demo emits the important watermark from the event stream itself.
                        }
                    })
                    .withTimestampAssigner((inputRecord, ts) -> inputRecord.timestamp)
            )
            .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .allowedLateness(Duration.ofSeconds(5))        // allow lateness of 5 seconds
            .sideOutputLateData(lateTag)
            .process(new SumAllWindowFunction());

        outputWindowedStream.print("output >>>");
        outputWindowedStream.getSideOutput(lateTag).print("late-records >>>");

        JobClient jobClient = env.executeAsync("LatenessNoAcceptSideOutputDemo");

        System.out.println("Job submitted, waiting for completion...");

        // Get the final result
        try {
            JobExecutionResult result = jobClient.getJobExecutionResult().get();
            System.out.println("Job finished with runtime: " + result.getNetRuntime() + " ms");
        } catch (Exception e) {
            System.err.println("Failed to get job execution result: " + e.getMessage());
        }
    }

    
    private static class SumAllWindowFunction extends ProcessAllWindowFunction<ElectronicOrder, Double, TimeWindow> {

        @Override
        public void process(
                Context ctx,
                Iterable<ElectronicOrder> inputs,
                Collector<Double> out) 
        {
            int subtaskIndex = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
            int numSubtasks = getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks();

            LocalDateTime windowStart = Instant.ofEpochMilli(ctx.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            LocalDateTime windowEnd = Instant.ofEpochMilli(ctx.window().getEnd()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            int batchSize = ((Collection<ElectronicOrder>) inputs).size();

            System.out.println("[" + windowStart + " - " + windowEnd + "], process records batch of: " + batchSize + ", subtask=" + subtaskIndex + "/" + numSubtasks);
            
            double totalPrice = 0.00;
            for (ElectronicOrder input : inputs) {
                System.out.println("\tProcessing input record: " + input.toString());
                totalPrice += input.price;
            }
            out.collect(totalPrice);
        }
    }
}
