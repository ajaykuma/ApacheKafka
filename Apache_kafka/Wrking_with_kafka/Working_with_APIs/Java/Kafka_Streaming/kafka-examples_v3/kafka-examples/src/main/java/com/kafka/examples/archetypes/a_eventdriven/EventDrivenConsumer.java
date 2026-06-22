package com.kafka.examples.archetypes.a_eventdriven;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * SECTION 2A – Event-Driven Consumer
 * =====================================
 * Demonstrates idiomatic Kafka consumer patterns:
 *
 *   1. Manual offset commit (commit AFTER successful processing)
 *   2. Dead-Letter Topic (DLT) for poison pill records
 *   3. Graceful shutdown with shutdown hook
 *   4. Each consumer subscribes to the relevant domain topic
 *
 * In a real microservice, each service would be a separate JVM with its own
 * consumer group ID. Here we demonstrate both in sequence.
 *
 * HOW TO RUN (after EventDrivenProducer):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.a_eventdriven.EventDrivenConsumer"
 */
public class EventDrivenConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventDrivenConsumer.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_ACCOUNT_DELETED,
                KafkaConfig.TOPIC_VEHICLE_MAPPED,
                KafkaConfig.TOPIC_ORDER_PLACED,
                KafkaConfig.TOPIC_INGESTION_DLT
        );

        // Simulate 3 separate microservices consuming the same events:
        // GDPR Service reacts to account deletion
        var gdprThread = Thread.ofVirtual().start(() ->
                runConsumer("gdpr-cleanup-service", KafkaConfig.TOPIC_ACCOUNT_DELETED));

        // Notification service reacts to account deletion
        var notifThread = Thread.ofVirtual().start(() ->
                runConsumer("notification-service", KafkaConfig.TOPIC_ACCOUNT_DELETED));

        // Inventory service reacts to orders
        var inventoryThread = Thread.ofVirtual().start(() ->
                runConsumer("inventory-service", KafkaConfig.TOPIC_ORDER_PLACED));

        gdprThread.join();
        notifThread.join();
        inventoryThread.join();
    }

    private static void runConsumer(String groupId, String topic) {

        // Shared DLT producer for poison pills
        var dltProducer = new KafkaProducer<String, String>(KafkaConfig.producerProps());

        try (var consumer = new KafkaConsumer<String, String>(KafkaConfig.consumerProps(groupId));
             dltProducer) {

            consumer.subscribe(List.of(topic));
            log.info("[{}] Subscribed to topic: {}", groupId, topic);

            // Graceful shutdown hook — triggers on CTRL+C
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("[{}] Shutdown signal received, waking consumer...", groupId);
                consumer.wakeup(); // causes poll() to throw WakeupException
            }));

            // ── Poll loop ──────────────────────────────────────────────────
            // This is the canonical consumer loop pattern
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(1));

                if (records.isEmpty()) {
                    log.info("[{}] Waiting for records on {}...", groupId, topic);
                    try { Thread.sleep(500); } catch (InterruptedException e) { break; }
                    continue;
                }

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        processRecord(groupId, record);
                        //  BEST PRACTICE: commit offset AFTER successful processing
                        // This ensures at-least-once delivery: if processing fails,
                        // the record will be re-delivered on restart
                        consumer.commitSync();

                    } catch (Exception e) {
                        log.error("[{}] Processing failed for key={}: {}",
                                groupId, record.key(), e.getMessage());

                        //  BEST PRACTICE: send to Dead-Letter Topic, never silently skip
                        sendToDlt(dltProducer, record, e.getMessage());

                        // Still commit so we don't get stuck on this poison pill
                        consumer.commitSync();
                    }
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException e) {
            log.info("[{}] Consumer shutdown cleanly", groupId);
        }
    }

    private static void processRecord(String serviceId, ConsumerRecord<String, String> record) {
        var event = JsonUtil.fromJson(record.value(), JsonUtil.DomainEvent.class);

        // Simulate different processing per service
        switch (serviceId) {
            case "gdpr-cleanup-service" -> {
                log.info("[gdpr] Scheduling data deletion for key={} eventType={} timestamp={}",
                        record.key(), event.eventType(), event.timestamp());
                // In real code: call GDPR cleanup API, update DB, etc.
            }
            case "notification-service" -> {
                log.info("[notifications] Sending deletion confirmation email for key={}", record.key());
                // In real code: call email service
            }
            case "inventory-service" -> {
                log.info("[inventory] Reserving stock for orderId={} eventType={}",
                        record.key(), event.eventType());
                // In real code: decrement inventory, check stock levels
            }
            default -> log.info("[{}] Processed: key={} offset={}",
                    serviceId, record.key(), record.offset());
        }
    }

    private static void sendToDlt(KafkaProducer<String, String> dltProducer,
                                   ConsumerRecord<String, String> original,
                                   String errorMessage) {
        // DLT record preserves original key and value, adds error context
        var dltRecord = new ProducerRecord<>(
                KafkaConfig.TOPIC_INGESTION_DLT,
                original.key(),
                String.format("""
                        {"originalTopic":"%s","originalOffset":%d,"error":"%s","originalValue":%s}""",
                        original.topic(), original.offset(), errorMessage, original.value())
        );
        dltProducer.send(dltRecord, (meta, ex) -> {
            if (ex == null)
                log.warn(" Sent to DLT: key={} at offset={}", original.key(), meta.offset());
        });
    }
}
