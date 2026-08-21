package com.example.kafka.kafkaconsumer.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * ADDITIONAL LISTENER — Dead Letter Topic Monitor
 * ===================================================
 * Watches "spring.orders.events.DLT" — the topic DeadLetterPublishingRecoverer
 * automatically routes failed records to (configured in JsonConsumerConfig).
 *
 * In production this listener would typically:
 *   - Send an alert (Slack, PagerDuty, email) when records appear
 *   - Log to a monitoring system for dashboards
 *   - Optionally attempt automated remediation for known failure patterns
 *
 * Note the value type is Object, not OrderEvent — DLT records may contain
 * deserialization exceptions or malformed payloads that don't fit the
 * OrderEvent schema, so we deliberately keep this loose.
 */
@Component
public class DltMonitorListener {

    private static final Logger log = LoggerFactory.getLogger(DltMonitorListener.class);

    @KafkaListener(
            topics = "spring.orders.events.DLT",
            groupId = "dlt-monitor-group",
            containerFactory = "orderKafkaListenerContainerFactory"
    )
    public void monitorDlt(ConsumerRecord<String, Object> record, Acknowledgment acknowledgment) {
        log.error("💀 DLT RECORD DETECTED — key={} partition={} offset={}",
                record.key(), record.partition(), record.offset());
        log.error("   value={}", record.value());
        log.error("   headers={}", record.headers());

        // In production: trigger alert here
        // alertService.notify("DLT record detected", record);

        acknowledgment.acknowledge();
    }
}
