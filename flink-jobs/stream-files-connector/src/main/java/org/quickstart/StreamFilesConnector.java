package org.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.common.JobBaseCommon;

public class StreamFilesConnector {
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());

        // Here you would set up your Flink streaming job to read from files
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Usually it's better to use a distributed file system like HDFS or S3 for production, but for simplicity, we use local files here.
        // Make sure to create the input file with some integer values (one per line) at /tmp/sample_data/input.txt before running this job.
        // Update permissions for the sample_data directory to ensure Flink can read/write files:
        // sudo chown -R flink:flink /tmp/sample_data
        // sudo chmod -R u+rwX /tmp/sample_data
        /**
         * IMPORTANT: When running this job in a distributed environment, ensure that the input file is accessible to all nodes 
         * (e.g., by using HDFS or S3) and update the file paths accordingly.
         */
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(), new Path("file:///tmp/sample_data/input.txt")).build();

        DataStream<String> inputText = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(),
                "input");

        DataStream<Integer> parsed = inputText.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) {
                return Integer.parseInt(value) + 1; // Simple transformation: parse the integer and add 1
            }
        });

        parsed.sinkTo(
                FileSink.forRowFormat(
                        new Path("file:///tmp/sample_data/output"),
                        new SimpleStringEncoder<Integer>()).build());

        parsed.print();
        JobClient jobClient = env.executeAsync("StreamFilesConnector Job");
        
        System.out.println("Job submitted, waiting for completion...");

        // Poll job status until completion
        JobStatus finalStatus = pollJobStatusUntilTerminal(jobClient);

        // Get the final result
        try {
            JobExecutionResult result = jobClient.getJobExecutionResult().get();
            System.out.println("Job finished with runtime: " + result.getNetRuntime() + " ms");
        } catch (Exception e) {
            System.err.println("Failed to get job execution result: " + e.getMessage());
        }
        System.out.println("Final job status: " + finalStatus);
    }

    /**
     * Polls the job status until it reaches a terminal state (FINISHED, FAILED, or CANCELED).
     * Prints status changes and handles exceptions.
     * 
     * @param jobClient The JobClient to poll
     * @return The final JobStatus, or null if initial status couldn't be retrieved
     */
    private static JobStatus pollJobStatusUntilTerminal(JobClient jobClient) {
        JobStatus currentStatus;
        try {
            currentStatus = jobClient.getJobStatus().get();
            System.out.println("Initial job status: " + currentStatus);
        } catch (Exception e) {
            System.err.println("Failed to get initial job status: " + e.getMessage());
            return null;
        }

        while (!(currentStatus == JobStatus.FINISHED || currentStatus == JobStatus.FAILED || currentStatus == JobStatus.CANCELED)) {
            try {
                Thread.sleep(50); // Poll every 50ms
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Polling interrupted: " + e.getMessage());
                break;
            }
            try {
                JobStatus newStatus = jobClient.getJobStatus().get();
                if (!newStatus.equals(currentStatus)) {
                    System.out.println("Job status changed to: " + newStatus);
                    currentStatus = newStatus;
                }
            } catch (Exception e) {
                System.err.println("Failed to poll job status: " + e.getMessage());
                break;
            }
        }

        return currentStatus;
    }
}
