package com.kafka.examples.archetypes.c_ingestion;

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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SECTION 2C – Ingestion & Distribution Layer
 * =============================================
 * Simulates the enterprise pattern of ingesting events from external systems
 * (ServiceNow, SAP) via a REST-like ingestion interface into Kafka.
 *
 * Pattern:
 *   External System (ServiceNow/SAP)
 *         │  HTTP POST / REST Proxy / Connector
 *         ▼
 *   [integration.servicenow.inbound]  ← high-volume inbound topic (~1.5–2M msg/day)
 *         │
 *         │  Consumer validates + routes
 *         ├─→ [routing by category: INCIDENT / CHANGE / PROBLEM]
 *         └─→ [integration.servicenow.inbound.DLT]  ← malformed / unprocessable
 *
 * Key concepts demonstrated:
 *   - Batched ingestion (simulate REST Proxy batch behaviour)
 *   - Schema validation at ingestion boundary
 *   - Routing by message category
 *   - Dead-letter topic for bad records
 *
 * HOW TO RUN:
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.c_ingestion.IngestionPipeline"
 */
public class IngestionPipeline {

    private static final Logger log = LoggerFactory.getLogger(IngestionPipeline.class);

    // Simulated batch size (REST Proxy typically sends batches of N records per POST)
    private static final int BATCH_SIZE = 10;

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_SERVICENOW_INBOUND,
                KafkaConfig.TOPIC_INGESTION_DLT
        );

        // Run ingestion and processing concurrently
        Thread.ofVirtual().start(IngestionPipeline::simulateRestProxyIngestion);
        Thread.sleep(1000); // let some records land first
        Thread.ofVirtual().start(IngestionPipeline::processAndRoute).join();
    }

    // ── Simulate REST Proxy Ingestion ──────────────────────────────────────
    // In production: ServiceNow or SAP posts to Confluent REST Proxy which
    // writes to Kafka. Here we directly produce, mimicking that behaviour.
    private static void simulateRestProxyIngestion() {
        log.info(" Simulating REST Proxy ingestion (ServiceNow → Kafka)...");

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            var random = new Random();
            var categories = List.of("INCIDENT", "CHANGE", "PROBLEM");
            var severities = List.of("P1", "P2", "P3", "P4");
            var teams      = List.of("Platform", "Network", "Application", "Database", "Security");

            int totalSent = 0;
            // Simulate 3 batches (as if from 3 REST Proxy POST calls)
            for (int batch = 1; batch <= 3; batch++) {

                log.info(" Sending batch {} of {}", batch, 3);

                for (int i = 0; i < BATCH_SIZE; i++) {
                    String incidentId = "INC-" + String.format("%06d", totalSent + i + 1);
                    String category   = categories.get(random.nextInt(categories.size()));
                    String severity   = severities.get(random.nextInt(severities.size()));
                    String team       = teams.get(random.nextInt(teams.size()));

                    ServiceNowIncident incident;
                    // Inject 2 bad records to demonstrate DLT
                    if (totalSent + i == 5 || totalSent + i == 17) {
                        // Malformed: missing required fields (simulate corrupted REST payload)
                        var raw = String.format("""
                            {"incidentId":"%s","category":null,"severity":"UNKNOWN"}
                            """, incidentId).trim();

                        producer.send(new ProducerRecord<>(
                                KafkaConfig.TOPIC_SERVICENOW_INBOUND,
                                incidentId,
                                raw  // invalid — will go to DLT
                        ));
                        log.warn(" Injected malformed record: {}", incidentId);
                    } else {
                        incident = new ServiceNowIncident(
                                incidentId, category, severity,
                                "Issue detected in " + team + " subsystem",
                                team + "-Team"
                        );
                        producer.send(new ProducerRecord<>(
                                KafkaConfig.TOPIC_SERVICENOW_INBOUND,
                                incidentId,
                                JsonUtil.toJson(incident)
                        ));
                    }
                }

                totalSent += BATCH_SIZE;
                producer.flush();
                log.info(" Batch {} flushed ({} records total)", batch, totalSent);
                Thread.sleep(500);
            }
            log.info(" Ingestion complete. {} records sent.", totalSent);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── Process, Validate & Route ──────────────────────────────────────────
    private static void processAndRoute() {
        log.info(" Starting ingestion processor...");

        var counters = new HashMap<String, AtomicInteger>();
        counters.put("INCIDENT", new AtomicInteger());
        counters.put("CHANGE",   new AtomicInteger());
        counters.put("PROBLEM",  new AtomicInteger());
        counters.put("DLT",      new AtomicInteger());

        try (var consumer = new KafkaConsumer<String, String>(
                     KafkaConfig.consumerProps("servicenow-ingestion-processor"));
             var dltProducer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            consumer.subscribe(List.of(KafkaConfig.TOPIC_SERVICENOW_INBOUND));
            int processed = 0;
            int target    = 30; // 3 batches × 10

            while (processed < target) {
                var records = consumer.poll(Duration.ofSeconds(2));

                for (ConsumerRecord<String, String> record : records) {
                    try {
                        // BEST PRACTICE: Validate schema at ingestion boundary
                        var incident = validateAndParse(record.value());

                        // Route by category to different downstream handlers
                        routeIncident(incident, record.key());
                        counters.get(incident.category()).incrementAndGet();

                    } catch (ValidationException e) {
                        // BEST PRACTICE: Never silently drop — send to DLT
                        log.error("Validation failed for {}: {}", record.key(), e.getMessage());
                        sendToDlt(dltProducer, record, e.getMessage());
                        counters.get("DLT").incrementAndGet();
                    } catch (Exception e) {
                        log.error("Unexpected error for {}: {}", record.key(), e.getMessage());
                        sendToDlt(dltProducer, record, "UNEXPECTED: " + e.getMessage());
                        counters.get("DLT").incrementAndGet();
                    }
                    processed++;
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

            log.info("=== Ingestion Processing Summary ===");
            counters.forEach((k, v) -> log.info("  {}: {} records", k, v.get()));
        }
    }

    // ── Schema validation at ingestion boundary ────────────────────────────
    private static ServiceNowIncident validateAndParse(String json) throws ValidationException {
        ServiceNowIncident incident;
        try {
            incident = JsonUtil.fromJson(json, ServiceNowIncident.class);
        } catch (Exception e) {
            throw new ValidationException("Unparseable JSON: " + e.getMessage());
        }
        if (incident.category() == null || incident.category().isBlank()) {
            throw new ValidationException("Missing required field: category");
        }
        if (!List.of("INCIDENT", "CHANGE", "PROBLEM").contains(incident.category())) {
            throw new ValidationException("Invalid category: " + incident.category());
        }
        if (incident.severity() == null || !incident.severity().startsWith("P")) {
            throw new ValidationException("Invalid severity: " + incident.severity());
        }
        return incident;
    }

    private static void routeIncident(ServiceNowIncident incident, String key) {
        // In production each case would forward to a different topic / service
        switch (incident.category()) {
            case "INCIDENT" -> log.info("[INCIDENT] {} | sev={} | team={}",
                    key, incident.severity(), incident.assignedTeam());
            case "CHANGE"   -> log.info("[CHANGE]   {} | sev={} | team={}",
                    key, incident.severity(), incident.assignedTeam());
            case "PROBLEM"  -> log.info("[PROBLEM]  {} | sev={} | team={}",
                    key, incident.severity(), incident.assignedTeam());
        }
    }

    private static void sendToDlt(KafkaProducer<String, String> dltProducer,
                                   ConsumerRecord<String, String> original,
                                   String reason) {
        var dltValue = String.format(
                "{\"key\":\"%s\",\"reason\":\"%s\",\"originalValue\":%s}",
                original.key(), reason, original.value());
        dltProducer.send(new ProducerRecord<>(
                KafkaConfig.TOPIC_INGESTION_DLT, original.key(), dltValue),
                (meta, ex) -> {
                    if (ex == null)
                        log.warn(" DLT ← key={} reason={}", original.key(), reason);
                });
    }

    static class ValidationException extends Exception {
        ValidationException(String msg) { super(msg); }
    }
}
