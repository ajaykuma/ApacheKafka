package com.kafkaops.performance;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Demonstrates the main consumer-side throughput knobs:
 *
 *  fetch.min.bytes     - broker won't respond until this much data is
 *                        available (or fetch.max.wait.ms elapses).
 *                        Higher = fewer, bigger fetches = better throughput,
 *                        worse latency on low-volume topics.
 *  fetch.max.wait.ms   - max time broker waits to satisfy fetch.min.bytes.
 *  max.poll.records    - caps how many records one poll() returns. Lower
 *                        values = more predictable processing time per
 *                        poll loop (useful to avoid hitting
 *                        max.poll.interval.ms and triggering a rebalance).
 *  max.partition.fetch.bytes - per-partition cap on fetched bytes; matters
 *                        when partitions have very large messages.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.performance.ConsumerTuningDemo \
 *       -Dexec.args="localhost:9092 perf-test-topic"
 */
public class ConsumerTuningDemo {

    public static void main(String[] args) {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "perf-test-topic";

        System.out.println("=== Default-ish profile (small fetches) ===");
        runProfile(bootstrapServers, topic, "tuning-demo-small", smallFetchProps(bootstrapServers));

        System.out.println("\n=== High-throughput profile (large fetches) ===");
        runProfile(bootstrapServers, topic, "tuning-demo-large", highThroughputProps(bootstrapServers));
    }

    private static Properties smallFetchProps(String bootstrapServers) {
        Properties props = baseProps(bootstrapServers);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1");
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        return props;
    }

    private static Properties highThroughputProps(String bootstrapServers) {
        Properties props = baseProps(bootstrapServers);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "65536"); // wait for 64KB
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "2000");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "10485760"); // 10MB
        return props;
    }

    private static Properties baseProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }

    private static void runProfile(String bootstrapServers, String topic, String groupId, Properties props) {
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));

            long start = System.currentTimeMillis();
            int totalRecords = 0;
            int emptyPolls = 0;

            // Run a fixed number of poll cycles or until we've seen enough empty
            // polls in a row (meaning we've drained the topic).
            while (emptyPolls < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    totalRecords += records.count();
                }
            }
            consumer.commitSync();

            long elapsed = System.currentTimeMillis() - start;
            System.out.println("Consumed " + totalRecords + " records in " + elapsed + " ms (group=" + groupId + ")");
        }
    }
}
