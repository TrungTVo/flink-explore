package org.quickstart;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import org.apache.flink.api.common.JobExecutionResult;
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

public class ProcessAllWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        String start = "2026-01-01T00:00:00Z";

        DataStream<ElectronicOrder> input = env.fromData(
            new ElectronicOrder("111", "trung", "HDTV", 2000.00, Instant.parse(start).toEpochMilli()),
            new ElectronicOrder("222", "aiko", "HDTV", 1999.23, Instant.parse(start).plusSeconds(4).toEpochMilli()),
            new ElectronicOrder("333", "trung", "ABCD", 4500.00, Instant.parse(start).plusSeconds(8).toEpochMilli()),
            new ElectronicOrder("444", "aiko", "ABCD", 1333.98, Instant.parse(start).plusSeconds(12).toEpochMilli()),
            new ElectronicOrder("555", "trung", "HDTV", 5000.98, Instant.parse(start).plusSeconds(16).toEpochMilli())
        );

        /** process based on event time */
        AllWindowedStream<ElectronicOrder, TimeWindow> evtWindowedStream = input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ElectronicOrder>forMonotonousTimestamps().withTimestampAssigner((inputRecord, ts) -> inputRecord.timestamp)
            )
            .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(10)));

        evtWindowedStream.process(new SumAllWindowFunction()).print("output");

        JobClient jobClient = env.executeAsync("ProcessAllWindowFunctionDemo");

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
