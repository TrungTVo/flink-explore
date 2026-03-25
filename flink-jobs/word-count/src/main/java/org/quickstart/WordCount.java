package org.quickstart;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.common.JobBaseCommon;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> wordCountStream = env.socketTextStream("taskmanager", 9999)
            .flatMap(new Splitter(), Types.TUPLE(Types.STRING, Types.INT))
            .keyBy((Tuple2<String, Integer> pair) -> pair.f0, Types.STRING)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(20)))
            .process(new CountWindowFunction());
        
        wordCountStream.print("output");
        
        JobClient jobClient = env.executeAsync("WordCount Job");
        
        System.out.println("Job submitted, waiting for completion...");

        // Get the final result
        try {
            JobExecutionResult result = jobClient.getJobExecutionResult().get();
            System.out.println("Job finished with runtime: " + result.getNetRuntime() + " ms");
        } catch (Exception e) {
            System.err.println("Failed to get job execution result: " + e.getMessage());
        }
    }

    private static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word : line.split("\\s+")) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }

    private static class CountWindowFunction extends ProcessWindowFunction<
        Tuple2<String, Integer>,    // IN
        Tuple2<String, Integer>,    // OUT
        String,                     // KEY
        TimeWindow
    > {
        @Override
        public void process(String wordKey,
                Context ctx,
                Iterable<Tuple2<String, Integer>> wordPairs, 
                Collector<Tuple2<String, Integer>> out
        ) throws Exception {
            LocalDateTime windowStart = Instant.ofEpochMilli(ctx.window().getStart()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            LocalDateTime windowEnd = Instant.ofEpochMilli(ctx.window().getEnd()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            int batchSize = ((Collection<Tuple2<String, Integer>>) wordPairs).size();

            System.out.println(">> " + wordKey + ", window = [" + windowStart + " - " + windowEnd + "], process records batch of: " + batchSize);
            int count = 0;
            for (Tuple2<String, Integer> pair : wordPairs) {
                System.out.println("\tprocess pair: " + pair);
                count += pair.f1;
            }
            out.collect(new Tuple2<>(wordKey, count));
        }
        
    }
}
