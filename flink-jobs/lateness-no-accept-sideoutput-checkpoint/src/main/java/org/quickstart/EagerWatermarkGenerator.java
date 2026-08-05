package org.quickstart;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.common.models.ElectronicOrder;

/**
 * Advances the watermark immediately to the highest observed event timestamp.
 */
final class EagerWatermarkGenerator implements WatermarkGenerator<ElectronicOrder> {

    private long maxTimestamp = Long.MIN_VALUE;

    @Override
    public void onEvent(
            ElectronicOrder event,
            long eventTimestamp,
            WatermarkOutput output) {
        maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        output.emitWatermark(new Watermark(maxTimestamp));
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // This demo eagerly emits a watermark for every event.
    }
}
