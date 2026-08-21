package com.kafkaops.troubleshooting;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Demonstrates the #1 cause of "mystery rebalances" in production: a consumer
 * whose per-poll processing time exceeds max.poll.interval.ms. The broker
 * assumes the consumer is dead, kicks it out of the group, and triggers a
 * rebalance — even though the process is still alive and healthy, just slow.
 *
 * This class deliberately sleeps inside the poll loop to reproduce it, with
 * a low max.poll.interval.ms so you can see it happen quickly instead of
 * waiting out the 5-minute default.
 *
 * Watch the logs (slf4j-simple prints to stdout) for:
 *   "Member ... sending LeaveGroup request to coordinator ... due to consumer poll timeout"
 *
 * Fixes in real life:
 *  - Increase max.poll.interval.ms if processing is legitimately slow.
 *  - Move slow work off the poll thread (hand off to a worker pool, commit
 *    after async completion).
 *  - Reduce max.poll.records so each poll has less work to finish in time.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.RebalanceTriggerDemo \
 *       -Dexec.args="localhost:9092 perf-test-topic"
 */
public class RebalanceTriggerDemo {

    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "perf-test-topic";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rebalance-demo-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Deliberately short so the demo doesn't take 5 minutes to reproduce.
        // DO NOT set this low in production unless you've measured your real
        // worst-case processing time per poll batch.
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "10000"); // 10s
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "50");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                    System.out.println(">>> REBALANCE: partitions revoked: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                    System.out.println(">>> REBALANCE: partitions assigned: " + partitions);
                }
            });

            System.out.println("Consuming with max.poll.interval.ms=10000 and an artificial 15s " +
                    "processing delay per batch — expect a rebalance within ~10-15s.");

            for (int cycle = 0; cycle < 5; cycle++) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                System.out.println("Polled " + records.count() + " records, now simulating slow processing...");

                // THIS is the bug being demonstrated: blocking the poll loop for
                // longer than max.poll.interval.ms.
                try {
                    Thread.sleep(15000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
}
