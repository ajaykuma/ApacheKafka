package com.kafka.examples.advanced.eos;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * ADVANCED – Exactly-Once Semantics (EOS): Transactional Producer
 * =================================================================
 * Demonstrates atomic multi-topic writes using Kafka transactions.
 *
 * THE PROBLEM without transactions:
 *   When a producer writes to two topics (e.g. order confirmed + inventory reserved),
 *   a crash between the two writes leaves the system in an inconsistent state:
 *   - Topic A has the record, Topic B does not
 *   - Retrying causes DUPLICATES on Topic A
 *
 * THE SOLUTION — Kafka Transactions:
 *   All writes within beginTransaction() / commitTransaction() are atomic.
 *   Either ALL records land on ALL topics, or NONE do.
 *   Consumers configured with isolation.level=read_committed never see partial writes.
 *
 * Key producer config:
 *   transactional.id   → unique per producer instance, enables transactions
 *   enable.idempotence → automatically true when transactional.id is set
 *   acks=all           → automatically set when transactional.id is set
 *
 * HOW TO RUN:
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.eos.TransactionalProducer"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.eos.TransactionVerifier"
 */
public class TransactionalProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionalProducer.class);

    // Two topics that must be written atomically — both or neither
    public static final String TOPIC_ORDERS_CONFIRMED  = "eos.orders.confirmed";
    public static final String TOPIC_INVENTORY_RESERVED = "eos.inventory.reserved";
    public static final String TOPIC_PAYMENTS_INITIATED = "eos.payments.initiated";

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                TOPIC_ORDERS_CONFIRMED,
                TOPIC_INVENTORY_RESERVED,
                TOPIC_PAYMENTS_INITIATED
        );

        try (var producer = new KafkaProducer<String, String>(transactionalProducerProps())) {

            // REQUIRED: must call initTransactions() once before any transaction
            // This registers the transactional.id with the broker's transaction coordinator
            producer.initTransactions();
            log.info(" Transactions initialized for transactional.id=order-processor-txn-1");

            // ── Scenario 1: Successful transaction (all 3 topics written atomically) ──
            log.info("\n--- Scenario 1: Successful atomic write across 3 topics ---");
            processOrderWithTransaction(producer, "ORD-TXN-001", "CUST-100", "PROD-A1", 2, 149.99, false);

            Thread.sleep(500);

            // ── Scenario 2: Another successful transaction ──
            log.info("\n--- Scenario 2: Another successful transaction ---");
            processOrderWithTransaction(producer, "ORD-TXN-002", "CUST-101", "PROD-B3", 1, 299.00, false);

            Thread.sleep(500);

            // ── Scenario 3: Simulated failure → transaction ABORTED ──
            // This demonstrates what happens when processing fails mid-transaction.
            // The abortTransaction() call rolls back ALL writes in this transaction.
            // Consumers with read_committed will NEVER see these records.
            log.info("\n--- Scenario 3: Simulated failure → transaction aborted ---");
            processOrderWithTransaction(producer, "ORD-TXN-003", "CUST-999", "PROD-INVALID", -1, 0.0, true);

            Thread.sleep(500);

            // ── Scenario 4: Successful transaction after the abort ──
            // Proves the producer recovers cleanly after an abort
            log.info("\n--- Scenario 4: Recovery — successful transaction after abort ---");
            processOrderWithTransaction(producer, "ORD-TXN-004", "CUST-102", "PROD-C7", 5, 49.95, false);

            log.info("\n All transactions complete.");
            log.info("   Run TransactionVerifier to confirm exactly 3 orders landed (not 4).");
            log.info("   ORD-TXN-003 was aborted and should be invisible to read_committed consumers.");
        }
    }

    /**
     * Processes an order by writing atomically to 3 topics:
     *   1. eos.orders.confirmed     → order confirmation event
     *   2. eos.inventory.reserved   → inventory reservation event
     *   3. eos.payments.initiated   → payment initiation event
     *
     * If simulateFailure=true, aborts after writing to topic 1 only.
     * With transactions, the partial write is invisible to read_committed consumers.
     */
    private static void processOrderWithTransaction(
            KafkaProducer<String, String> producer,
            String orderId, String customerId, String productId,
            int quantity, double amount, boolean simulateFailure) {

        producer.beginTransaction();
        log.info("[TXN] BEGIN transaction for orderId={}", orderId);

        try {
            // Write 1: order confirmed
            var orderEvent = DomainEvent.of("OrderConfirmed", "order-service",
                    new OrderPlacedEvent(orderId, customerId, productId, quantity, amount, "CONFIRMED"));
            producer.send(new ProducerRecord<>(TOPIC_ORDERS_CONFIRMED, orderId, JsonUtil.toJson(orderEvent)));
            log.info("[TXN] Wrote to {}: orderId={}", TOPIC_ORDERS_CONFIRMED, orderId);

            // Simulate a failure AFTER first write but BEFORE second write
            // Without transactions: eos.orders.confirmed would have the record but
            // eos.inventory.reserved would not → inconsistent state
            if (simulateFailure) {
                throw new RuntimeException("Simulated failure: invalid product " + productId);
            }

            // Write 2: inventory reserved
            var inventoryEvent = String.format(
                    "{\"orderId\":\"%s\",\"productId\":\"%s\",\"quantity\":%d,\"reservedAt\":\"%s\"}",
                    orderId, productId, quantity, java.time.Instant.now());
            producer.send(new ProducerRecord<>(TOPIC_INVENTORY_RESERVED, orderId, inventoryEvent));
            log.info("[TXN] Wrote to {}: orderId={}", TOPIC_INVENTORY_RESERVED, orderId);

            // Write 3: payment initiated
            var paymentEvent = String.format(
                    "{\"orderId\":\"%s\",\"customerId\":\"%s\",\"amount\":%.2f,\"currency\":\"EUR\"}",
                    orderId, customerId, amount);
            producer.send(new ProducerRecord<>(TOPIC_PAYMENTS_INITIATED, orderId, paymentEvent));
            log.info("[TXN] Wrote to {}: orderId={}", TOPIC_PAYMENTS_INITIATED, orderId);

            // All 3 writes succeeded → COMMIT
            producer.commitTransaction();
            log.info("[TXN]  COMMITTED transaction for orderId={}", orderId);

        } catch (Exception e) {
            // Something went wrong → ABORT the entire transaction
            // All writes within this transaction are rolled back atomically
            log.error("[TXN] ❌ ABORTING transaction for orderId={}: {}", orderId, e.getMessage());
            producer.abortTransaction();
            log.warn("[TXN]  Transaction aborted. No records visible to read_committed consumers.");
        }
    }

    // ── Transactional producer config ─────────────────────────────────────
    public static Properties transactionalProducerProps() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // THE key config for transactions:
        // transactional.id must be UNIQUE per producer instance and STABLE across restarts.
        // The broker uses it to fence zombie producers (old crashed instances).
        // Convention: use a meaningful name tied to the processing logic.
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "order-processor-txn-1");

        // These are automatically set when transactional.id is present,
        // but explicit for clarity:
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        return props;
    }
}
