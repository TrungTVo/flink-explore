package org.quickstart;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.common.models.ElectronicOrder;


public class BoundedOutOfOrderDemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(4);

        // every 500 ms, ask the watermark strategy whether it can emit a new watermark
        /**
         * Why it matters here:
         * With fromData(...), records are consumed extremely fast. The whole source may finish before the periodic watermark tick happens, 
         * so you might not see intermediate firings. With socketTextStream(...), you manually type events slowly. Setting the interval 
         * to 500 ms makes Flink check frequently enough that after you type
         */
        env.getConfig().setAutoWatermarkInterval(500L);

        System.out.println("Parallelism: " + env.getParallelism());

        String start = "2026-01-01T00:00:00Z";

        System.out.println("Start a socket in the taskmanager container with: nc -lk 9999");
        System.out.println("Input format: orderId,userId,electronicId,price,eventOffsetSeconds");

        DataStream<ElectronicOrder> input = env.socketTextStream("taskmanager", 9999)
            .map(new SocketOrderParser(start));

        /** process based on event time */
        AllWindowedStream<ElectronicOrder, TimeWindow> evtWindowedStream = input.assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<ElectronicOrder>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((inputRecord, ts) -> inputRecord.timestamp)
            )
            .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
            .allowedLateness(Duration.ofSeconds(5));        // allow lateness of 5 seconds

        evtWindowedStream.process(new SumAllWindowFunction()).print("output");

        JobClient jobClient = env.executeAsync("BoundedOutOfOrderDemo");

        System.out.println("Job submitted, waiting for completion...");

        // Get the final result
        try {
            JobExecutionResult result = jobClient.getJobExecutionResult().get();
            System.out.println("Job finished with runtime: " + result.getNetRuntime() + " ms");
        } catch (Exception e) {
            System.err.println("Failed to get job execution result: " + e.getMessage());
        }
    }

    private static class SocketOrderParser implements MapFunction<String, ElectronicOrder> {
        private final long startTimestamp;

        private SocketOrderParser(String start) {
            this.startTimestamp = Instant.parse(start).toEpochMilli();
        }

        @Override
        public ElectronicOrder map(String inputRecord) {
            String[] recordData = inputRecord.split(",");
            if (recordData.length != 5) {
                throw new IllegalArgumentException(
                    "Expected input format: orderId,userId,electronicId,price,eventOffsetSeconds");
            }

            String orderId = recordData[0].trim();
            String userId = recordData[1].trim();
            String electronicId = recordData[2].trim();
            double price = Double.parseDouble(recordData[3].trim());
            long eventOffsetMillis = Duration.ofSeconds(Long.parseLong(recordData[4].trim())).toMillis();

            return new ElectronicOrder(orderId, userId, electronicId, price, startTimestamp + eventOffsetMillis);
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
