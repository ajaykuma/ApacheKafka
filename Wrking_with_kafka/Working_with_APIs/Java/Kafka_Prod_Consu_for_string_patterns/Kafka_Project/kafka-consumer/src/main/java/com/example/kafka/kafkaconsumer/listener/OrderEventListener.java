package com.example.kafka.kafkaconsumer.listener;

import com.example.kafka.kafkaconsumer.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * ADDITIONAL LISTENER — Order Processing with Manual Ack + Retry/DLT
 * ======================================================================
 * Listens to "spring.orders.events" using the orderKafkaListenerContainerFactory
 * bean (JSON deserialization, manual ack, DefaultErrorHandler with retry+DLT).
 *
 * Compare this to your existing ConsumerController.consumer() method:
 *   - Your original: auto-commit, plain String, no error handling
 *   - This one: manual ack, JSON POJO, automatic retry + DLT on failure
 *
 * Test the retry+DLT behaviour by sending an order with productId="FAIL"
 * (see the validation check below) — watch it retry 3 times then land
 * on "spring.orders.events.DLT".
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    @KafkaListener(
            topics = "spring.orders.events",
            groupId = "order-processing-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void processOrder(
            ConsumerRecord<String, OrderEvent> record,
            Acknowledgment acknowledgment) {

        OrderEvent order = record.value();
        log.info("📨 Received orderId={} partition={} offset={}",
                order.getOrderId(), record.partition(), record.offset());

        try {
            // ── Simulated business validation ───────────────────────────
            // In production this might be: DB constraint check, external
            // API validation, business rule enforcement, etc.
            validateOrder(order);

            // ── Simulated processing ─────────────────────────────────────
            processOrderBusinessLogic(order);

            log.info("✅ Successfully processed orderId={}", order.getOrderId());

            // CRITICAL: only acknowledge AFTER successful processing.
            // If we crash before this line, the record will be redelivered
            // on restart (at-least-once delivery guarantee).
            acknowledgment.acknowledge();

        } catch (Exception e) {
            log.error("❌ Processing failed for orderId={}: {}", order.getOrderId(), e.getMessage());

            // DO NOT acknowledge here. Re-throw so DefaultErrorHandler catches it,
            // applies the retry/backoff policy, and eventually routes to DLT
            // if all retries are exhausted. The error handler manages the
            // offset commit in that case — we must NOT call acknowledge()
            // on the failure path or we'd skip the retry/DLT mechanism entirely.
            throw new RuntimeException("Order processing failed: " + order.getOrderId(), e);
        }
    }

    private void validateOrder(OrderEvent order) {
        // Deliberate failure trigger for testing retry + DLT behaviour.
        // Send a request with productId=FAIL to see this fire.
        if ("FAIL".equalsIgnoreCase(order.getProductId())) {
            throw new IllegalArgumentException("Simulated validation failure for productId=FAIL");
        }
        if (order.getAmount() <= 0) {
            throw new IllegalArgumentException("Invalid amount: " + order.getAmount());
        }
        if (order.getQuantity() <= 0) {
            throw new IllegalArgumentException("Invalid quantity: " + order.getQuantity());
        }
    }

    private void processOrderBusinessLogic(OrderEvent order) {
        // Placeholder for real business logic: save to DB, call another
        // service, update inventory, etc.
        log.info("   Processing: {}", order);
    }
}
