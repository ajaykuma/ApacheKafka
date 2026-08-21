package com.kafka.examples.archetypes.b_streaming;

import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Reads from the enriched orders topic to verify the pipeline output.
 *
 * HOW TO RUN (while OrderEnrichmentPipeline is running):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.b_streaming.EnrichmentVerifier"
 */
public class EnrichmentVerifier {

    private static final Logger log = LoggerFactory.getLogger(EnrichmentVerifier.class);

    public static void main(String[] args) {
        try (var consumer = new KafkaConsumer<String, String>(
                KafkaConfig.consumerProps("enrichment-verifier-group"))) {

            consumer.subscribe(List.of(KafkaConfig.TOPIC_ENRICHED_ORDERS));
            log.info("Listening for enriched orders on: {}", KafkaConfig.TOPIC_ENRICHED_ORDERS);

            int received = 0;
            while (received < 4) {
                var records = consumer.poll(Duration.ofSeconds(3));
                for (var record : records) {
                    received++;
                    log.info(" ENRICHED ORDER #{} → key={}\n   value={}\n",
                            received, record.key(), record.value());
                    consumer.commitSync();
                }
                if (records.isEmpty()) {
                    log.info("Still waiting... (is OrderEnrichmentPipeline running?)");
                }
            }
            log.info(" Received all enriched orders.");
        }
    }
}
