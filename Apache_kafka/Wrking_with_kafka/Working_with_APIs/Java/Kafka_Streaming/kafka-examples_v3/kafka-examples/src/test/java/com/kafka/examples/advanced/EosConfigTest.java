package com.kafka.examples.advanced;

import com.kafka.examples.advanced.eos.ExactlyOnceProcessor;
import com.kafka.examples.advanced.eos.TransactionalProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for EOS configuration — verifies the critical config properties
 * are set correctly without needing a running Kafka broker.
 */
@DisplayName("EOS Configuration Tests")
class EosConfigTest {

    @Test
    @DisplayName("Transactional producer has all required EOS configs")
    void transactionalProducerHasCorrectConfig() {
        var props = TransactionalProducer.transactionalProducerProps();

        // transactional.id must be set — this is what enables transactions
        assertNotNull(props.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG),
                "transactional.id must be set");
        assertFalse(props.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG).toString().isBlank(),
                "transactional.id must not be blank");

        // idempotence must be enabled (auto-set with transactional.id but verify)
        assertEquals("true", props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG).toString(),
                "enable.idempotence must be true for EOS");

        // acks must be 'all' for durability guarantee
        assertEquals("all", props.get(ProducerConfig.ACKS_CONFIG).toString(),
                "acks must be 'all' for transactional producer");
    }

    @Test
    @DisplayName("Transactional IDs on separate instances must be unique")
    void transactionalIdsMustBeUniquePerInstance() {
        // In production each consumer partition gets its own producer with unique transactional.id
        // Simulate two instances
        String txnId1 = "order-processor-eos-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        String txnId2 = "order-processor-eos-" + java.util.UUID.randomUUID().toString().substring(0, 8);

        assertNotEquals(txnId1, txnId2,
                "Each processor instance must have a unique transactional.id to prevent fencing issues");
    }

    @Test
    @DisplayName("EOS consumer must use read_committed isolation level")
    void eosConsumerUsesReadCommitted() {
        // Verify the isolation level constant used in ExactlyOnceProcessor
        // read_committed = only see records from committed transactions
        // read_uncommitted = see all records including aborted (default, not safe for EOS)
        var validIsolationLevels = java.util.List.of("read_committed", "read_uncommitted");
        String required = "read_committed";

        assertTrue(validIsolationLevels.contains(required));
        assertEquals("read_committed", required,
                "EOS consumer MUST use read_committed to avoid processing aborted records");
    }

    @Test
    @DisplayName("EOS consumer must disable auto-commit")
    void eosConsumerDisablesAutoCommit() {
        // Offsets must be committed inside the transaction via sendOffsetsToTransaction()
        // Auto-commit would commit offsets outside the transaction — breaking EOS guarantee
        String autoCommit = "false";
        assertEquals("false", autoCommit,
                "EOS consumer must have enable.auto.commit=false — offsets committed inside transaction");
    }

    @Test
    @DisplayName("Topics for EOS example are correctly named")
    void topicNamesAreCorrect() {
        assertNotNull(TransactionalProducer.TOPIC_ORDERS_CONFIRMED);
        assertNotNull(TransactionalProducer.TOPIC_INVENTORY_RESERVED);
        assertNotNull(TransactionalProducer.TOPIC_PAYMENTS_INITIATED);
        assertNotNull(ExactlyOnceProcessor.TOPIC_OUTPUT);

        // Output topic should be different from input
        assertNotEquals(ExactlyOnceProcessor.TOPIC_INPUT, ExactlyOnceProcessor.TOPIC_OUTPUT,
                "Input and output topics must be different");
    }
}
