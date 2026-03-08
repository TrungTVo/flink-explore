package org.quickstart;

import java.time.Duration;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;
import org.common.JobBaseCommon;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> wordCountStream = env.socketTextStream("localhost", 9999)
            .flatMap(new Splitter(), Types.TUPLE(Types.STRING, Types.INT))
            .keyBy((Tuple2<String, Integer> pair) -> pair.f0, Types.STRING)
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(20)))
            .sum(1);

        wordCountStream.print();
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
}
