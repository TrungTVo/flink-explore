package org.quickstart;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.common.models.ElectronicOrder;

/**
 * Creates the event-time strategy used by the lateness example.
 */
final class EagerWatermarkStrategyFactory {

    private EagerWatermarkStrategyFactory() {}

    static WatermarkStrategy<ElectronicOrder> create() {
        return WatermarkStrategy
                .<ElectronicOrder>forGenerator(
                        context -> new EagerWatermarkGenerator())
                .withTimestampAssigner(
                        (order, previousTimestamp) -> order.timestamp);
    }
}
