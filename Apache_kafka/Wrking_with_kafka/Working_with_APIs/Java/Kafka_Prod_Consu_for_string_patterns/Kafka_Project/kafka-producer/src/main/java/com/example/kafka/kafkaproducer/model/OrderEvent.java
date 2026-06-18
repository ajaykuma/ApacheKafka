package com.example.kafka.kafkaproducer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import java.time.Instant;

/**
 * Domain model representing an order event.
 * Used to demonstrate JSON serialization with Spring Kafka (instead of plain String).
 *
 * Spring Kafka uses Jackson under the hood when configured with JsonSerializer.
 * This POJO will be automatically converted to JSON on send() and back to POJO
 * on the consumer side (see OrderEvent in the consumer project).
 */
public class OrderEvent {

    private String orderId;
    private String customerId;
    private String productId;
    private int quantity;
    private double amount;
    private String status;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant createdAt;

    public OrderEvent() {
        // Required no-arg constructor for Jackson deserialization
    }

    public OrderEvent(String orderId, String customerId, String productId,
                       int quantity, double amount, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.status = status;
        this.createdAt = Instant.now();
    }

    // ── Getters and setters (required by Jackson) ──────────────────────────
    public String getOrderId() { return orderId; }
    public void setOrderId(String orderId) { this.orderId = orderId; }

    public String getCustomerId() { return customerId; }
    public void setCustomerId(String customerId) { this.customerId = customerId; }

    public String getProductId() { return productId; }
    public void setProductId(String productId) { this.productId = productId; }

    public int getQuantity() { return quantity; }
    public void setQuantity(int quantity) { this.quantity = quantity; }

    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    @Override
    public String toString() {
        return "OrderEvent{orderId='%s', customerId='%s', productId='%s', quantity=%d, amount=%.2f, status='%s'}"
                .formatted(orderId, customerId, productId, quantity, amount, status);
    }
}
