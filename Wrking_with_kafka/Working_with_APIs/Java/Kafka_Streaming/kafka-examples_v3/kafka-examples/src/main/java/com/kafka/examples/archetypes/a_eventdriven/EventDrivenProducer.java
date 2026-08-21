package com.kafka.examples.archetypes.a_eventdriven;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
//import java.util.concurrent.TimeUnit;

/**
 * SECTION 2A – Event-Driven Microservices
 * =========================================
 * Demonstrates the pub/sub pattern for domain events.
 *
 * Pattern:
 *   - Each domain action publishes a structured event to a Kafka topic
 *   - Topic is named after the domain event:  <domain>.<entity>.<verb>
 *   - Producer does NOT know who consumes the event (loose coupling)
 *   - The event KEY is always the entity ID (customerId, vehicleId, etc.)
 *     so records for the same entity go to the same partition (ordering guarantee)
 *
 * Run this first, then run EventDrivenConsumer to see the events consumed.
 *
 * HOW TO RUN: From Project folder <terminal/cmd>
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.a_eventdriven.EventDrivenProducer"
 */
public class EventDrivenProducer {

    private static final Logger log = LoggerFactory.getLogger(EventDrivenProducer.class);

    public static void main(String[] args) throws Exception {

        // Create topics first
        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_ACCOUNT_DELETED,
                KafkaConfig.TOPIC_VEHICLE_MAPPED,
                KafkaConfig.TOPIC_ORDER_PLACED
        );

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            // ── Event 1: Account Deletion ──────────────────────────────────
            // Triggered when a customer requests account deletion.
            // Multiple downstream services react: notification, GDPR cleanup, vehicle unmapping.
            log.info("--- Publishing account.deleted events ---");

            var deletions = List.of(
                    new AccountDeletedEvent("CUST-001", "alice@example.com", "USER_REQUEST", "alice"),
                    new AccountDeletedEvent("CUST-002", "bob@example.com",   "ADMIN_ACTION", "admin"),
                    new AccountDeletedEvent("CUST-003", "carol@example.com", "USER_REQUEST", "carol")
            );

            for (var deletion : deletions) {
                var event = DomainEvent.of("AccountDeleted", "customer-service", deletion);
                // KEY = customerId → ensures all events for the same customer go to the same partition
                var record = new ProducerRecord<>(
                        KafkaConfig.TOPIC_ACCOUNT_DELETED,
                        deletion.customerId(),       // KEY
                        JsonUtil.toJson(event)        // VALUE
                );

                // Async send with callback for confirmation
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send account.deleted for {}", deletion.customerId(), ex);
                    } else {
                        log.info(" account.deleted → topic={} partition={} offset={} customerId={}",
                                metadata.topic(), metadata.partition(), metadata.offset(), deletion.customerId());
                    }
                });
            }

            // ── Event 2: Vehicle Mapped to Customer ────────────────────────
            // Published when a vehicle is associated with a customer account.
            log.info("--- Publishing customer.vehicle.mapped events ---");

            var mappings = List.of(
                    new VehicleMappedEvent("VH-001", "CUST-100", "WBA12345", "2023", "BMW"),
                    new VehicleMappedEvent("VH-002", "CUST-101", "WBA67890", "2024", "BMW"),
                    new VehicleMappedEvent("VH-003", "CUST-100", "WBA11111", "2022", "MINI") // same customer, 2nd vehicle
            );

            for (var mapping : mappings) {
                var event = DomainEvent.of("VehicleMapped", "vehicle-service", mapping);
                var record = new ProducerRecord<>(
                        KafkaConfig.TOPIC_VEHICLE_MAPPED,
                        mapping.customerId(),        // KEY = customerId for consumer grouping
                        JsonUtil.toJson(event)
                );
                producer.send(record, (metadata, ex) -> {
                    if (ex != null) {
                        log.error("Failed to send vehicle.mapped", ex);
                    } else {
                        log.info(" vehicle.mapped → partition={} offset={} vehicleId={} customerId={}",
                                metadata.partition(), metadata.offset(), mapping.vehicleId(), mapping.customerId());
                    }
                });
            }

            // ── Event 3: Order Placed ──────────────────────────────────────
            log.info("--- Publishing logistics.order.placed events ---");

            var orders = List.of(
                    new OrderPlacedEvent("ORD-001", "CUST-100", "PROD-A1", 2, 149.99, "PENDING"),
                    new OrderPlacedEvent("ORD-002", "CUST-101", "PROD-B3", 1, 299.00, "PENDING"),
                    new OrderPlacedEvent("ORD-003", "CUST-100", "PROD-C7", 5,  49.95, "PENDING")
            );

            for (var order : orders) {
                var event = DomainEvent.of("OrderPlaced", "order-service", order);
                var record = new ProducerRecord<>(
                        KafkaConfig.TOPIC_ORDER_PLACED,
                        order.orderId(),             // KEY = orderId
                        JsonUtil.toJson(event)
                );
                producer.send(record, (meta, ex) -> {
                    if (ex == null)
                        log.info(" order.placed → partition={} offset={}", meta.partition(), meta.offset());
                });
            }

            // Flush ensures all async sends complete before closing
            producer.flush();
            log.info(" All domain events published. Run EventDrivenConsumer to consume them.");
        }
    }
}
