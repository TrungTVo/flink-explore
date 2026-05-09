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
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.common.models.ElectronicOrder;


public class LatenessAcceptDemo {

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
                Instant.parse(start).plusSeconds(14).toEpochMilli()),                             // event time 00:00:14
            
            // This record belongs to the first window, but arrives late.
            new ElectronicOrder("333", "trung", "ABCD", 4500.00, 
                Instant.parse(start).plusSeconds(8).toEpochMilli())                               // event time 00:00:08
        );

        /** process based on event time */
        AllWindowedStream<ElectronicOrder, TimeWindow> evtWindowedStream = input.assignTimestampsAndWatermarks(
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
            .allowedLateness(Duration.ofSeconds(5));        // allow lateness of 5 seconds

        evtWindowedStream.process(new SumAllWindowFunction()).print("output");

        JobClient jobClient = env.executeAsync("LatenessAcceptDemo");

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
