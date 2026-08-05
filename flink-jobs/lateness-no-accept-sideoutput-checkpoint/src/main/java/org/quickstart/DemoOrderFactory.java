package org.quickstart;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.common.models.ElectronicOrder;

/**
 * Builds the deterministic input sequence used by the demonstration.
 */
final class DemoOrderFactory {

    private static final String START = "2026-01-01T00:00:00Z";
    private static final String FAILURE_MARKER_ID = "FAILURE_MARKER";

    private DemoOrderFactory() {}

    static List<ElectronicOrder> createOrders(boolean includeFailureMarker) {
        Instant start = Instant.parse(START);
        List<ElectronicOrder> orders = new ArrayList<>();
        orders.add(new ElectronicOrder(
                "111", "trung", "HDTV", 2000.00, start.toEpochMilli()));
        orders.add(new ElectronicOrder(
                "222", "aiko", "HDTV", 1999.23, start.plusSeconds(4).toEpochMilli()));
        orders.add(new ElectronicOrder(
                "444", "aiko", "ABCD", 1333.98, start.plusSeconds(12).toEpochMilli()));
        orders.add(new ElectronicOrder(
                "555", "trung", "HDTV", 5000.98, start.plusSeconds(18).toEpochMilli()));

        if (includeFailureMarker) {
            orders.add(new ElectronicOrder(
                    FAILURE_MARKER_ID,
                    "demo",
                    "control",
                    0.00,
                    start.plusSeconds(18).toEpochMilli()));
        }

        orders.add(new ElectronicOrder(
                "333", "trung", "ABCD", 4500.00, start.plusSeconds(8).toEpochMilli()));
        return orders;
    }

    static boolean isFailureMarker(ElectronicOrder order) {
        return FAILURE_MARKER_ID.equals(order.order_id);
    }
}
