package org.common.models;

/**
 * For simplicity, we are using public fields here.
 * In a production environment, you might want to use private fields with
 * getters and setters, or even a builder pattern for immutability.
 */
public class ElectronicOrder {
    public String order_id;
    public String user_id;
    public String electronic_id;
    public Double price;
    public Long timestamp;

    public ElectronicOrder(
            String order_id,
            String user_id,
            String electronic_id,
            Double price,
            Long timestamp) {
        this.order_id = order_id;
        this.user_id = user_id;
        this.electronic_id = electronic_id;
        this.price = price;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return String.format("ElectronicOrder{electronic_id=%s, user_id=%s, order_id=%s, price=%.2f}",
                electronic_id, user_id, order_id, price);
    }
}
