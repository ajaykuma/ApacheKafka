package com.example.kafka.kafkaconsumer.listener;

import com.example.kafka.kafkaconsumer.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * ADDITIONAL LISTENER — Batch Processing
 * ==========================================
 * Same topic as OrderEventListener, but a DIFFERENT consumer group, so this
 * receives the SAME messages independently (fan-out pattern, like your 2A
 * gdpr-cleanup-service / notification-service example, but for Spring).
 *
 * Notice the method signature: List<ConsumerRecord<...>> instead of a single
 * ConsumerRecord. Spring delivers up to MAX_POLL_RECORDS_CONFIG (50, configured
 * in BatchConsumerConfig) records in one call.
 *
 * Use case: bulk insert into a database, bulk API call, aggregation before
 * writing — anywhere the overhead of per-record processing dominates.
 */
@Component
public class BatchOrderListener {

    private static final Logger log = LoggerFactory.getLogger(BatchOrderListener.class);

    @KafkaListener(
            topics = "spring.orders.events",
            groupId = "order-batch-processing-group",
            containerFactory = "batchKafkaListenerContainerFactory"
    )
    public void processBatch(
            List<ConsumerRecord<String, OrderEvent>> records,
            Acknowledgment acknowledgment) {

        log.info("📦 Batch received: {} records", records.size());

        double batchTotal = 0;
        int validCount = 0;

        for (ConsumerRecord<String, OrderEvent> record : records) {
            OrderEvent order = record.value();
            if (order == null) {
                log.warn("⚠️ Skipping null/malformed record at offset={}", record.offset());
                continue;
            }
            batchTotal += order.getAmount();
            validCount++;
            log.info("   orderId={} customerId={} amount={}",
                    order.getOrderId(), order.getCustomerId(), order.getAmount());
        }

        // ── Simulated bulk operation ─────────────────────────────────────
        // In production: one bulk INSERT instead of N individual INSERTs.
        log.info("💾 Bulk-processing {} valid orders, total amount={}", validCount, batchTotal);

        // Acknowledge the ENTIRE batch at once. If this throws before
        // reaching here, Spring will redeliver the WHOLE batch on restart
        // (batch-level at-least-once, not per-record).
        acknowledgment.acknowledge();
        log.info("✅ Batch of {} records committed", records.size());
    }
}
