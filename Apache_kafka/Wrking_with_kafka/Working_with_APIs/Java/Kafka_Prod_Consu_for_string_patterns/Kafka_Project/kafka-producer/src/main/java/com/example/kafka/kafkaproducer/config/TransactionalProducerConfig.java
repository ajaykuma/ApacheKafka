package com.example.kafka.kafkaproducer.config;

import com.example.kafka.kafkaproducer.model.OrderEvent;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * ADDITIONAL CONFIG — Transactional Producer (Exactly-Once Semantics)
 * ======================================================================
 * Demonstrates Spring's wrapper around Kafka transactions — the same EOS
 * concept from the EOS plain-Java examples, but using Spring's @Transactional
 * style instead of manual beginTransaction()/commitTransaction() calls.
 *
 * Use case: write to BOTH "orders" and "inventory" topics atomically.
 * If anything fails mid-way, BOTH writes are rolled back — no partial state.
 *
 * KEY DIFFERENCE FROM PLAIN KAFKA CLIENT:
 *   Plain Kafka:  producer.beginTransaction() / commitTransaction() / abortTransaction()
 *   Spring Kafka: @Transactional annotation on the service method —
 *                 Spring manages begin/commit/abort automatically based on
 *                 whether the method throws an exception.
 */
@Configuration
public class TransactionalProducerConfig {

    @Bean
    public ProducerFactory<String, OrderEvent> transactionalProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        var factory = new DefaultKafkaProducerFactory<String, OrderEvent>(config);

        // THE key setting for transactions: every producer created by this
        // factory will have a UNIQUE transactional.id derived from this prefix.
        // Spring appends a unique suffix automatically per producer instance.
        factory.setTransactionIdPrefix("order-tx-");

        return factory;
    }

    @Bean
    public KafkaTemplate<String, OrderEvent> transactionalKafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    // ── Transaction Manager ────────────────────────────────────────────────
    // This is what makes @Transactional work on service methods.
    // Spring uses this to begin/commit/abort transactions automatically
    // based on whether the annotated method completes normally or throws.
    @Bean
    public KafkaTransactionManager<String, OrderEvent> kafkaTransactionManager(
            ProducerFactory<String, OrderEvent> transactionalProducerFactory) {
        return new KafkaTransactionManager<>(transactionalProducerFactory);
    }
}
