package org.quickstart;

import java.time.Duration;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.util.OutputTag;
import org.common.models.ElectronicOrder;

/**
 * Assembles the source and operators for the checkpoint demonstration.
 */
final class CheckpointDemoPipeline {

    private CheckpointDemoPipeline() {}

    static SingleOutputStreamOperator<Double> build(
            StreamExecutionEnvironment env,
            DemoOptions options,
            OutputTag<ElectronicOrder> lateTag) {
        DataStream<ElectronicOrder> input = env
                .fromData(DemoOrderFactory.createOrders(options.failureDemo()))
                .name("bounded-orders")
                .uid("bounded-orders");

        DataStream<ElectronicOrder> recoveredInput = input
                .map(new RecoveryProbe(options.failureDemo()))
                .name("recovery-probe-and-failure-injector")
                .uid("recovery-probe-and-failure-injector");

        return recoveredInput
                .assignTimestampsAndWatermarks(
                        EagerWatermarkStrategyFactory.create())
                .name("eager-watermarks")
                .uid("eager-watermarks")
                // The marker advances the watermark after recovery, but must not
                // become part of a business window.
                .filter(order -> !DemoOrderFactory.isFailureMarker(order))
                .name("remove-failure-marker")
                .uid("remove-failure-marker")
                .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .allowedLateness(Duration.ofSeconds(5))
                .sideOutputLateData(lateTag)
                .process(new SumAllWindowFunction())
                .name("sum-all-window")
                .uid("sum-all-window");
    }
}
