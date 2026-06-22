package com.kafka.examples.advanced.errorhandling;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Produces a mix of valid, invalid, and malformed records to exercise
 * all error handling paths in StreamsErrorHandling.
 *
 * Record types:
 *   VALID       → correct JSON, positive amount/quantity   → goes to output topic
 *   INVALID     → correct JSON but fails business rules    → goes to DLT with reason
 *   MALFORMED   → not valid JSON                          → goes to DLT
 *   EMPTY       → blank value                             → goes to DLT
 *
 * HOW TO RUN (while StreamsErrorHandling is running):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.errorhandling.ErrorProducer"
 */
public class ErrorProducer {

    private static final Logger log = LoggerFactory.getLogger(ErrorProducer.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(StreamsErrorHandling.TOPIC_INPUT);

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            // ── Valid orders → should appear in output topic ───────────────
            log.info("Publishing VALID orders...");
            List.of(
                new OrderPlacedEvent("ORD-EH-001", "CUST-100", "PROD-A1", 2,  149.99, "PENDING"),
                new OrderPlacedEvent("ORD-EH-002", "CUST-101", "PROD-B3", 1,  299.00, "PENDING"),
                new OrderPlacedEvent("ORD-EH-003", "CUST-102", "PROD-C7", 5,   49.95, "PENDING"),
                new OrderPlacedEvent("ORD-EH-004", "CUST-103", "PROD-D2", 1, 1299.00, "PENDING")
            ).forEach(order -> {
                producer.send(new ProducerRecord<>(
                        StreamsErrorHandling.TOPIC_INPUT,
                        order.orderId(),
                        JsonUtil.toJson(order)
                ), (m, e) -> { if (e == null) log.info("📤 VALID: {}", order.orderId()); });
            });

            Thread.sleep(300);

            // ── Invalid: negative amount → DLT with INVALID_AMOUNT reason ─
            log.info("Publishing INVALID orders (business rule violations)...");
            List.of(
                new OrderPlacedEvent("ORD-EH-005", "CUST-100", "PROD-X1", 2,   -50.00, "PENDING"), // negative amount
                new OrderPlacedEvent("ORD-EH-006", "CUST-101", "PROD-X2", -1,   99.00, "PENDING"), // negative quantity
                new OrderPlacedEvent("ORD-EH-007", null,        "PROD-X3",  1,   25.00, "PENDING"), // null customerId
                new OrderPlacedEvent("ORD-EH-008", "CUST-102", "PROD-X4",  1,    0.00, "PENDING")  // zero amount
            ).forEach(order -> {
                producer.send(new ProducerRecord<>(
                        StreamsErrorHandling.TOPIC_INPUT,
                        order.orderId(),
                        JsonUtil.toJson(order)
                ), (m, e) -> { if (e == null) log.warn("📤 INVALID: {}", order.orderId()); });
            });

            Thread.sleep(300);

            // ── Malformed: not valid JSON → DLT with PARSE_ERROR reason ───
            log.info("Publishing MALFORMED records (not JSON)...");
            List.of(
                new String[]{"ORD-EH-009", "NOT_JSON_AT_ALL"},
                new String[]{"ORD-EH-010", "{{broken json::"},
                new String[]{"ORD-EH-011", ""},           // empty string
                new String[]{"ORD-EH-012", "null"}        // literal null string
            ).forEach(pair -> {
                producer.send(new ProducerRecord<>(
                        StreamsErrorHandling.TOPIC_INPUT,
                        pair[0],
                        pair[1]
                ), (m, e) -> { if (e == null) log.warn("📤 MALFORMED: {}", pair[0]); });
            });

            Thread.sleep(300);

            // ── More valid orders after the bad ones ───────────────────────
            // Proves the topology continues processing after encountering bad records
            log.info("Publishing more VALID orders (proves topology survived bad records)...");
            List.of(
                new OrderPlacedEvent("ORD-EH-013", "CUST-100", "PROD-E9", 3,  79.99, "PENDING"),
                new OrderPlacedEvent("ORD-EH-014", "CUST-101", "PROD-F1", 1, 199.00, "PENDING")
            ).forEach(order -> {
                producer.send(new ProducerRecord<>(
                        StreamsErrorHandling.TOPIC_INPUT,
                        order.orderId(),
                        JsonUtil.toJson(order)
                ), (m, e) -> { if (e == null) log.info("📤 VALID (post-error): {}", order.orderId()); });
            });

            producer.flush();
            log.info("\n All records published.");
            log.info("   Expected in output topic: ORD-EH-001,002,003,004,013,014 (6 valid)");
            log.info("   Expected in DLT topic:    ORD-EH-005,006,007,008,009,010,011,012 (8 invalid)");
        }
    }
}
