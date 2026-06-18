package com.example.kafka.kafkaconsumer.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/**
 * Mirrors the producer's OrderEvent. In a real microservices setup, producer
 * and consumer are SEPARATE codebases — this class is deliberately
 * independent (not shared via a common library) to reflect that reality.
 *
 * @JsonIgnoreProperties(ignoreUnknown = true) is important: if the producer
 * adds a new field later, this consumer won't crash trying to deserialize it.
 * This is a basic form of forward compatibility without a schema registry.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderEvent {

    private String orderId;
    private String customerId;
    private String productId;
    private int quantity;
    private double amount;
    private String status;

    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant createdAt;

    public OrderEvent() {}

    public OrderEvent(String orderId, String customerId, String productId,
                       int quantity, double amount, String status) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.productId = productId;
        this.quantity = quantity;
        this.amount = amount;
        this.status = status;
    }

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
