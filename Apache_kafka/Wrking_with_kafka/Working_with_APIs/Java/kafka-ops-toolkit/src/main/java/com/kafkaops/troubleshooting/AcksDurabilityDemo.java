package com.kafkaops.troubleshooting;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Shows the producer-side configuration that protects against the most
 * common data-loss scenario (see DATA_LOSS_SCENARIOS.md, item #1):
 * acks=1 acknowledging a write the leader hasn't replicated yet.
 *
 * This demo is safe to run against a single local broker — it won't kill
 * anything — it just prints the configs and sends a few records so you can
 * see acks=all + idempotence in action via the producer logs.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.AcksDurabilityDemo \
 *       -Dexec.args="localhost:9092 perf-test-topic"
 */
public class AcksDurabilityDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "perf-test-topic";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // --- Durability-first configuration ---
        // acks=all: leader waits for all in-sync replicas to ack before
        // confirming the write to the producer.
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        // enable.idempotence=true: prevents duplicate records on producer
        // retries (e.g. after a transient network blip), which also forces
        // acks=all and bounds max.in.flight.requests.per.connection<=5
        // automatically — it's the recommended default for anything that
        // cares about correctness.
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // retries: with idempotence on, safe to retry aggressively without
        // risking duplicates.
        props.put(ProducerConfig.RETRIES_CONFIG, "5");

        System.out.println("=== Durability-first producer config ===");
        System.out.println("acks=all, enable.idempotence=true, retries=5");
        System.out.println("(Pair this with topic-level min.insync.replicas=2 and replication.factor=3");
        System.out.println(" for real protection — acks=all alone only guarantees 'all CURRENT ISR members',");
        System.out.println(" which could be just 1 replica if min.insync.replicas isn't also set.)");
        System.out.println();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, "durability-demo-key", "durability-demo-value-" + i);

                // Always check the result — never fire-and-forget in real code
                // (see DATA_LOSS_SCENARIOS.md item #6).
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("Acked: partition=%d offset=%d%n", metadata.partition(), metadata.offset());
            }
        }
    }
}
