package com.example.kafka.kafkaproducer.service;

import com.example.kafka.kafkaproducer.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CompletableFuture;

/**
 * ADDITIONAL SERVICE — Order Service with multiple send patterns
 * ==================================================================
 * Demonstrates 5 distinct producer patterns on top of your existing setup:
 *
 *   1. sendJsonOrder()           → basic JSON send (replaces plain String)
 *   2. sendOrderAsync()          → async send with success/failure callback
 *   3. sendOrderWithKey()        → explicit key for partition control
 *   4. sendOrdersTransactionally → atomic multi-topic write (EOS)
 *   5. sendOrderFireAndForget()  → no callback, no wait (lowest latency, least safe)
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    public static final String TOPIC_ORDERS    = "spring.orders.events";
    public static final String TOPIC_INVENTORY = "spring.inventory.events";

    private final KafkaTemplate<String, OrderEvent> orderKafkaTemplate;
    private final KafkaTemplate<String, OrderEvent> transactionalKafkaTemplate;

    @Autowired
    public OrderService(
            @Qualifier("orderKafkaTemplate") KafkaTemplate<String, OrderEvent> orderKafkaTemplate,
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, OrderEvent> transactionalKafkaTemplate) {
        this.orderKafkaTemplate = orderKafkaTemplate;
        this.transactionalKafkaTemplate = transactionalKafkaTemplate;
    }

    // ── Pattern 1: Basic JSON send (synchronous-feeling, but still async) ──
    // send() always returns a CompletableFuture — it never blocks by default.
    // Here we just don't bother handling the future, which is risky in
    // production (silent failures) but common for low-stakes events.
    public void sendJsonOrder(OrderEvent order) {
        orderKafkaTemplate.send(TOPIC_ORDERS, order.getCustomerId(), order);
        log.info("📤 Sent (fire-and-forget style) orderId={}", order.getOrderId());
    }

    // ── Pattern 2: Async send WITH callback ─────────────────────────────────
    // This is the RECOMMENDED pattern for production. You get notified of
    // success (with partition/offset metadata) or failure (with exception)
    // without blocking the calling thread.
    public void sendOrderAsync(OrderEvent order) {
        CompletableFuture<org.springframework.kafka.support.SendResult<String, OrderEvent>> future =
                orderKafkaTemplate.send(TOPIC_ORDERS, order.getCustomerId(), order);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("✅ Async send succeeded: orderId={} partition={} offset={}",
                        order.getOrderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("❌ Async send FAILED for orderId={}: {}",
                        order.getOrderId(), ex.getMessage());
                // In production: retry, send to a fallback topic, alert, etc.
            }
        });
    }

    // ── Pattern 3: Explicit key for partition control ───────────────────────
    // Demonstrates that the KEY determines partition assignment — same key
    // always goes to the same partition, guaranteeing per-key ordering.
    // Here we deliberately use productId instead of customerId to show
    // a DIFFERENT partitioning strategy than your account/order examples.
    public void sendOrderWithKey(OrderEvent order, String partitionKey) {
        orderKafkaTemplate.send(TOPIC_ORDERS, partitionKey, order)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("📤 Sent with custom key='{}' → partition={}",
                                partitionKey, result.getRecordMetadata().partition());
                    }
                });
    }

    // ── Pattern 4: Transactional — atomic write to 2 topics ─────────────────
    // @Transactional here is bound to the KafkaTransactionManager bean.
    // Spring automatically:
    //   1. Calls beginTransaction() before this method runs
    //   2. Calls commitTransaction() if the method returns normally
    //   3. Calls abortTransaction() if the method throws ANY exception
    //
    // transactionManager value MUST match the bean name from
    // TransactionalProducerConfig (Spring infers "kafkaTransactionManager"
    // automatically since there's only one KafkaTransactionManager bean).
    @Transactional("kafkaTransactionManager")
    public void sendOrdersTransactionally(OrderEvent order, boolean simulateFailure) {
        log.info("[TXN] BEGIN — orderId={}", order.getOrderId());

        // Write 1: order confirmed
        transactionalKafkaTemplate.send(TOPIC_ORDERS, order.getCustomerId(), order);
        log.info("[TXN] Wrote to {}: orderId={}", TOPIC_ORDERS, order.getOrderId());

        if (simulateFailure) {
            // Throwing here causes Spring to ABORT the transaction.
            // The write above becomes invisible to read_committed consumers.
            throw new RuntimeException("Simulated failure for orderId=" + order.getOrderId());
        }

        // Write 2: inventory reservation (same order, different topic)
        var inventoryEvent = new OrderEvent(
                order.getOrderId(), order.getCustomerId(), order.getProductId(),
                order.getQuantity(), order.getAmount(), "INVENTORY_RESERVED");
        transactionalKafkaTemplate.send(TOPIC_INVENTORY, order.getOrderId(), inventoryEvent);
        log.info("[TXN] Wrote to {}: orderId={}", TOPIC_INVENTORY, order.getOrderId());

        log.info("[TXN] ✅ Method completing normally — Spring will COMMIT");
        // No explicit commit call needed — Spring handles it via the
        // KafkaTransactionManager when this method returns successfully.
    }

    // ── Pattern 5: True fire-and-forget (no future handling at all) ────────
    // Lowest latency, but you have ZERO visibility into success or failure.
    // Only appropriate for genuinely non-critical telemetry/logging events.
    public void sendOrderFireAndForget(OrderEvent order) {
        orderKafkaTemplate.send(TOPIC_ORDERS, order.getCustomerId(), order);
        // Deliberately not checking the returned future — "fire and forget"
    }
}
