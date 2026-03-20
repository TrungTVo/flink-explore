package org.quickstart;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.common.JobBaseCommon;
import org.common.models.ElectronicOrder;

public class BranchStreams {
    public static void main(String[] args) throws Exception {
        // Demonstrate usage of JobBaseCommon
        System.out.println(JobBaseCommon.getCommonMessage());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        System.out.println("Parallelism: " + env.getParallelism());

        DataStream<ElectronicOrder> electronicOrders = env.fromData(
                new ElectronicOrder("111", "trung", "HDTV", 2000.00, null),
                new ElectronicOrder("222", "aiko", "HDTV", 1999.23, null),
                new ElectronicOrder("333", "trung", "ABCD", 4500.00, null),
                new ElectronicOrder("444", "aiko", "ABCD", 1333.98, null),
                new ElectronicOrder("555", "trung", "HDTV", 5000.98, null));

        electronicOrders.keyBy((ElectronicOrder electronicOrder) -> electronicOrder.electronic_id, Types.STRING)
                .reduce((order1, order2) -> {
                    double totalPrice = order1.price + order2.price;
                    return new ElectronicOrder(
                            "common_order_id_b1",
                            "common_user_id_b1",
                            order1.electronic_id,
                            totalPrice,
                            null);
                })
                .print("b1 [electronic_id]");

        DataStream<ElectronicOrder> electronicOrders_traced_one_partition = electronicOrders
                .map(new RichMapFunction<ElectronicOrder, ElectronicOrder>() {
                    @Override
                    public ElectronicOrder map(ElectronicOrder value) {
                        String key = "common_key_partition";
                        int subtask = getRuntimeContext().getTaskInfo().getIndexOfThisSubtask();
                        System.out.println("assignedKey=" + key + ", currentSubtask=" + subtask + ", " + value.toString());
                        return value;
                    }
                });

        electronicOrders_traced_one_partition.keyBy((ElectronicOrder electronicOrder) -> "common_key_partition", Types.STRING)
                .reduce((order1, order2) -> {
                    double totalPrice = order1.price + order2.price;
                    return new ElectronicOrder(
                            "common_order_id_b2",
                            "common_user_id_b2",
                            "common_key_partition",
                            totalPrice,
                            null);
                })
                .print("b2 [common_key_partition]");

        JobClient jobClient = env.executeAsync("Branch Streams");

        System.out.println("Job submitted, waiting for completion...");

        // Get the final result
        try {
            JobExecutionResult result = jobClient.getJobExecutionResult().get();
            System.out.println("Job finished with runtime: " + result.getNetRuntime() + " ms");
        } catch (Exception e) {
            System.err.println("Failed to get job execution result: " + e.getMessage());
        }
    }

}
