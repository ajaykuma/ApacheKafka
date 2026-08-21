package com.kafka.examples.advanced.eos;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * ADVANCED – Exactly-Once Semantics: Consume-Transform-Produce (CTP)
 * ====================================================================
 * The most important EOS pattern in enterprise Kafka:
 * reading from one topic, transforming, and writing to another —
 * all within a SINGLE transaction so offsets and output are atomic.
 *
 * WITHOUT EOS (at-least-once):
 *   1. Consumer reads record, produces output
 *   2. Crash before offset commit
 *   3. On restart: record re-consumed → DUPLICATE output produced
 *
 * WITH EOS (exactly-once):
 *   1. Consumer reads record
 *   2. Producer begins transaction
 *   3. Producer writes output record
 *   4. Producer sends consumer offsets INTO the same transaction
 *      (sendOffsetsToTransaction — the key API call)
 *   5. Producer commits transaction
 *   → Output record AND offset commit are atomic.
 *   → On crash/restart: either both happened or neither did. No duplicates.
 *
 * Consumer config requirement:
 *   isolation.level=read_committed  → only reads committed transaction records
 *   enable.auto.commit=false        → offsets managed by the transaction
 *
 * HOW TO RUN:
 *   Prerequisite: run TransactionalProducer first to populate eos.orders.confirmed
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.eos.ExactlyOnceProcessor"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.eos.TransactionVerifier"
 */
public class ExactlyOnceProcessor {

    private static final Logger log = LoggerFactory.getLogger(ExactlyOnceProcessor.class);

    public static final String TOPIC_INPUT  = TransactionalProducer.TOPIC_ORDERS_CONFIRMED;
    public static final String TOPIC_OUTPUT = "eos.orders.processed";

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(TOPIC_INPUT, TOPIC_OUTPUT);

        // Each processor instance needs a UNIQUE transactional.id
        // In production: append the partition number or instance ID
        String transactionalId = "order-processor-eos-" + UUID.randomUUID().toString().substring(0, 8);

        var producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        var consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "eos-order-processor-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // CRITICAL: disable auto commit — offsets are committed inside the transaction
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // CRITICAL: only read records from committed transactions
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        try (var producer = new KafkaProducer<String, String>(producerProps);
             var consumer = new KafkaConsumer<String, String>(consumerProps)) {

            producer.initTransactions();
            log.info("EOS processor started. transactional.id={}", transactionalId);
            log.info("   Reading from: {}  Writing to: {}", TOPIC_INPUT, TOPIC_OUTPUT);
            log.info("   isolation.level=read_committed — aborted records are invisible");

            consumer.subscribe(List.of(TOPIC_INPUT));

            int processed = 0;
            int target = 3; // expect 3 committed orders (ORD-TXN-003 was aborted)

            while (processed < target) {
                var records = consumer.poll(Duration.ofSeconds(3));

                if (records.isEmpty()) {
                    log.info("Waiting for records... (run TransactionalProducer first)");
                    continue;
                }

                // Process each record in its own transaction
                for (ConsumerRecord<String, String> record : records) {

                    producer.beginTransaction();

                    try {
                        // ── Transform ──────────────────────────────────────
                        var event = JsonUtil.fromJson(record.value(), DomainEvent.class);
                        var processed_event = String.format(
                                "{\"orderId\":\"%s\",\"status\":\"PROCESSED\",\"processedAt\":\"%s\"," +
                                "\"originalEvent\":%s}",
                                record.key(), java.time.Instant.now(), record.value());

                        log.info("[EOS] Processing orderId={} partition={} offset={}",
                                record.key(), record.partition(), record.offset());

                        // ── Produce output ─────────────────────────────────
                        producer.send(new ProducerRecord<>(TOPIC_OUTPUT, record.key(), processed_event));

                        // ── THE KEY EOS API CALL: sendOffsetsToTransaction ─
                        // This atomically includes the consumer offset commit
                        // INSIDE the producer transaction.
                        // If the transaction commits → record processed + offset advanced (no reprocessing)
                        // If the transaction aborts → record NOT processed + offset NOT advanced (will retry)
                        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                        offsetsToCommit.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );

                        // Pass consumer group metadata for fencing (prevents zombie consumers)
                        producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());

                        producer.commitTransaction();
                        processed++;
                        log.info("[EOS] COMMITTED: orderId={} offset={} atomically advanced",
                                record.key(), record.offset());

                    } catch (Exception e) {
                        log.error("[EOS] Processing failed for orderId={}: {}", record.key(), e.getMessage());
                        producer.abortTransaction();
                        // Offset was NOT committed → record will be reprocessed on next poll
                        log.warn("[EOS] Transaction aborted. Record will be retried.");
                    }
                }
            }

            log.info("\n EOS processing complete. {} records processed exactly once.", processed);
            log.info("   Check {} for the output.", TOPIC_OUTPUT);
        }
    }
}
