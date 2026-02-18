package org.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
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
        env.execute("StreamFilesConnector Job");
    }
}
