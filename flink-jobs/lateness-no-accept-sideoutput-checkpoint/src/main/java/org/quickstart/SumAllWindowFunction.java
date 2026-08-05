package org.quickstart;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.common.models.ElectronicOrder;

/**
 * Logs each event-time window and emits its total order price.
 */
final class SumAllWindowFunction
        extends ProcessAllWindowFunction<ElectronicOrder, Double, TimeWindow> {

    @Override
    public void process(
            Context context,
            Iterable<ElectronicOrder> inputs,
            Collector<Double> out) {
        LocalDateTime windowStart = toUtcDateTime(context.window().getStart());
        LocalDateTime windowEnd = toUtcDateTime(context.window().getEnd());
        List<ElectronicOrder> records = copyRecords(inputs);

        System.out.printf(
                "[%s - %s], process records batch of: %d%n",
                windowStart, windowEnd, records.size());

        double totalPrice = sumAndLog(records);
        out.collect(totalPrice);
    }

    private static LocalDateTime toUtcDateTime(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis)
                .atZone(ZoneOffset.UTC)
                .toLocalDateTime();
    }

    private static List<ElectronicOrder> copyRecords(
            Iterable<ElectronicOrder> inputs) {
        List<ElectronicOrder> records = new ArrayList<>();
        inputs.forEach(records::add);
        return records;
    }

    private static double sumAndLog(List<ElectronicOrder> records) {
        double totalPrice = 0.00;
        for (ElectronicOrder input : records) {
            System.out.println("\tProcessing input record: " + input);
            totalPrice += input.price;
        }
        return totalPrice;
    }
}
