package org.quickstart;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.common.models.ElectronicOrder;

/**
 * Flink streaming job that computes the average order price per electronic product
 * using a 10-second tumbling event-time window with an incremental {@link AggregateFunction}.
 */
public class AggregateDemo {

    /**
     * Builds the Flink pipeline, submits the job asynchronously, and waits for completion.
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        String start = "2026-01-01T00:00:00Z";

        DataStream<ElectronicOrder> input = env.fromData(
            new ElectronicOrder("111", "trung", "HDTV", 2000.00, Instant.parse(start).toEpochMilli()),
            new ElectronicOrder("222", "aiko", "HDTV", 6000.00, Instant.parse(start).plusSeconds(4).toEpochMilli()),
            new ElectronicOrder("222", "aiko", "ABCD", 28000.00, Instant.parse(start).plusSeconds(4).toEpochMilli()),
            new ElectronicOrder("333", "trung", "ABCD", 4400.00, Instant.parse(start).plusSeconds(8).toEpochMilli()),
            new ElectronicOrder("444", "aiko", "ABCD", 1333.98, Instant.parse(start).plusSeconds(12).toEpochMilli()),
            new ElectronicOrder("555", "trung", "HDTV", 5000.98, Instant.parse(start).plusSeconds(16).toEpochMilli())
        );

        /** process based on event time */
        WindowedStream<ElectronicOrder, String, TimeWindow> evtWindowedStream = input.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ElectronicOrder>forMonotonousTimestamps().withTimestampAssigner((inputRecord, ts) -> inputRecord.timestamp)
            )
            .keyBy((ElectronicOrder order) -> order.electronic_id)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)));

        evtWindowedStream.aggregate(new AvgWindowFunction(), new AvgProcessWindowFunction())
            .print("output");

        JobClient jobClient = env.executeAsync("AggregateDemo");

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
     * Incrementally accumulates the sum and count of order prices within a window,
     * producing the average price as its final result.
     */
    private static class AvgWindowFunction implements AggregateFunction<ElectronicOrder, Tuple2<Double, Integer>, Double> {

        /** Returns a zeroed (sum=0.0, count=0) accumulator. */
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.00, 0);
        }

        /**
         * Adds one order's price to the running accumulator.
         *
         * @param order       incoming order record
         * @param accumulator current (sum, count) pair
         * @return updated accumulator
         */
        @Override
        public Tuple2<Double, Integer> add(ElectronicOrder order, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + order.price, accumulator.f1 + 1);
        }

        /**
         * Computes the average price from the final accumulator.
         *
         * @param accumulator final (sum, count) pair
         * @return average price
         */
        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        /**
         * Merges two partial accumulators by summing their totals and counts.
         *
         * @param acc1 first partial accumulator
         * @param acc2 second partial accumulator
         * @return combined accumulator
         */
        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> acc1, Tuple2<Double, Integer> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }


    /**
     * Receives the pre-aggregated average from {@link AvgWindowFunction}, logs the window
     * boundaries and batch size, then emits the average downstream.
     */
    private static class AvgProcessWindowFunction extends ProcessWindowFunction<Double, Double, String, TimeWindow> {

        /**
         * Logs the window key, time range, and record count, then forwards the average.
         *
         * @param key        the electronic product ID
         * @param ctx        window context providing start/end timestamps
         * @param runningAccs single-element iterable containing the pre-aggregated average
         * @param out         collector for the output average
         */
        @Override
        public void process(
                String key,
                Context ctx,
                Iterable<Double> runningAccs,
                Collector<Double> out)
        {
            LocalDateTime windowStart = Instant.ofEpochMilli(ctx.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            LocalDateTime windowEnd = Instant.ofEpochMilli(ctx.window().getEnd()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            int batchSize = ((Collection<Double>) runningAccs).size();      // always 1 with AggregateFunction

            Double runningAvg = runningAccs.iterator().next();          // with AggregateFunction, there should be only 1 result per window
            System.out.println("key=" + key + ", [" + windowStart + " - " + windowEnd + "], process records batch of: " + batchSize + ", average price: " + runningAvg);
            out.collect(runningAvg);
        }
    }

}
