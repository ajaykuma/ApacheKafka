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

import java.util.HashMap;
import java.util.Map;

/**
 * ADDITIONAL CONFIG — JSON Producer (alongside your existing String producer)
 * ==============================================================================
 * Your existing KafkaConfiguration.java produces String messages only.
 * This adds a SEPARATE KafkaTemplate that serializes POJOs to JSON automatically.
 *
 * Why a second KafkaTemplate?
 *   Spring Kafka ties serializer type to the ProducerFactory at creation time.
 *   You cannot mix String and JSON serialization on the same KafkaTemplate.
 *   In production you typically have ONE JSON template used everywhere,
 *   but this shows both side-by-side so you can compare.
 *
 * Key configs explained:
 *   JsonSerializer.ADD_TYPE_INFO_HEADERS = false
 *     → Without this, Spring adds a "__TypeId__" header with the full Java
 *       class name (com.example.kafka.kafkaproducer.model.OrderEvent).
 *       This COUPLES producer and consumer to the same package structure —
 *       a serious problem in microservices where producer and consumer are
 *       different services with different codebases.
 *     → Setting it to false sends pure JSON with no Java-specific metadata.
 *       The consumer then specifies the target type explicitly.
 *
 *   acks=all, retries, idempotence
 *     → Production-grade reliability settings, commented out in your original.
 *     → Explained in detail in code comments below.
 */
@Configuration
public class JsonProducerConfig {

    @Bean
    public ProducerFactory<String, OrderEvent> jsonProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // CRITICAL: prevents coupling to Java class names across services
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // ── Production reliability settings ────────────────────────────────
        // acks=all: wait for leader + all in-sync replicas to acknowledge.
        // Without this (acks=1 default), a leader crash right after ack
        // can lose the message even though the producer thinks it succeeded.
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        // enable.idempotence=true: prevents duplicate records caused by
        // producer-side retries (e.g. network blip causes retry, but the
        // original request actually succeeded — without idempotence you'd
        // get the same order TWICE).
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // retries: how many times to retry a failed send before giving up.
        // With idempotence=true, retries are safe (no duplicates).
        config.put(ProducerConfig.RETRIES_CONFIG, 3);

        // compression: reduces network and disk usage, especially valuable
        // for JSON payloads which are verbose compared to binary formats.
        config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, OrderEvent> orderKafkaTemplate() {
        return new KafkaTemplate<>(jsonProducerFactory());
    }
}
