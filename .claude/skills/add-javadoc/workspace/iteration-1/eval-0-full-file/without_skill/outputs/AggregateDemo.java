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
 * This class demonstrates the usage of Flink's AggregateFunction and ProcessWindowFunction
 * together to compute aggregate statistics on streaming data.
 *
 * <p>It sets up a streaming environment, creates a data stream of electronic orders,
 * assigns timestamps and watermarks for event-time processing, and applies windowed
 * aggregation to compute average prices.
 */
public class AggregateDemo {

    /**
     * The main entry point for the AggregateDemo Flink job.
     *
     * <p>This method sets up the streaming execution environment, creates a data stream
     * from sample electronic orders, applies event-time windowing, and uses an
     * AggregateFunction combined with a ProcessWindowFunction to compute and output
     * average prices per key.
     *
     * @param args command-line arguments (not used)
     * @throws Exception if the Flink job fails to execute
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
     * An implementation of Flink's {@link AggregateFunction} that computes the average
     * price of electronic orders within a window.
     *
     * <p>The accumulator stores a {@link Tuple2} containing the running sum of prices
     * (field f0) and the count of orders processed (field f1). The final result is
     * computed by dividing the sum by the count.
     */
    private static class AvgWindowFunction implements AggregateFunction<ElectronicOrder, Tuple2<Double, Integer>, Double> {

        /**
         * Creates and returns a new accumulator initialized with zero sum and zero count.
         *
         * @return a new {@link Tuple2} with f0=0.0 and f1=0
         */
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.00, 0);
        }

        /**
         * Adds an electronic order to the accumulator by adding its price to the running
         * sum and incrementing the count by one.
         *
         * @param order       the electronic order to add
         * @param accumulator the current accumulator state
         * @return a new accumulator with updated sum and count
         */
        @Override
        public Tuple2<Double, Integer> add(ElectronicOrder order, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + order.price, accumulator.f1 + 1);
        }

        /**
         * Returns the final result by computing the average price from the accumulator.
         *
         * @param accumulator the final accumulator state containing sum and count
         * @return the average price (sum / count)
         */
        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        /**
         * Merges two accumulators by combining their sums and counts.
         *
         * @param acc1 the first accumulator
         * @param acc2 the second accumulator
         * @return a new accumulator with the combined sum and count of both inputs
         */
        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> acc1, Tuple2<Double, Integer> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }


    /**
     * A {@link ProcessWindowFunction} that receives the pre-aggregated average price
     * from {@link AvgWindowFunction} and logs information about the window before
     * forwarding the result downstream.
     *
     * <p>This function is responsible for enriching the output with window metadata
     * such as start time, end time, and batch size.
     */
    private static class AvgProcessWindowFunction extends ProcessWindowFunction<Double, Double, String, TimeWindow> {

        /**
         * Processes the window result by logging window metadata and emitting the average.
         *
         * @param key         the key for this window (electronic product ID)
         * @param ctx         the window context providing access to window boundaries
         * @param runningAccs the iterable containing the pre-aggregated average value
         * @param out         the collector used to emit output records
         * @throws Exception if processing fails
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
