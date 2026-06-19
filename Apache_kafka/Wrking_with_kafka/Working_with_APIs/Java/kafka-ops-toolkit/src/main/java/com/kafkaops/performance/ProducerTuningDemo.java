package com.kafkaops.performance;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Side-by-side producer tuning demo: runs the same workload under a
 * "low-latency" profile and a "high-throughput" profile, and prints
 * wall-clock time for each so the tradeoff is directly observable.
 *
 * Key knobs and what they trade off:
 *
 *  linger.ms        - how long to wait to batch more records before sending.
 *                      0 = send immediately (low latency, more requests).
 *                      5-50ms = bigger batches (higher throughput, small added latency).
 *  batch.size       - max bytes per batch per partition. Larger = better
 *                      compression & fewer requests, but more memory and
 *                      a bit more latency per batch.
 *  compression.type - "none" vs "lz4"/"zstd": trades CPU for network/disk.
 *  acks             - "1" (leader only) vs "all" (full ISR): trades
 *                      durability for latency.
 *  buffer.memory    - total memory producer can use to queue records
 *                      before blocking the caller.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.performance.ProducerTuningDemo \
 *       -Dexec.args="localhost:9092 perf-test-topic 10000"
 */
public class ProducerTuningDemo {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "perf-test-topic";
        int recordCount = args.length > 2 ? Integer.parseInt(args[2]) : 10_000;

        System.out.println("=== Low-latency profile ===");
        long lowLatencyMs = runProfile(bootstrapServers, topic, recordCount, lowLatencyProps(bootstrapServers));

        System.out.println("\n=== High-throughput profile ===");
        long highThroughputMs = runProfile(bootstrapServers, topic, recordCount, highThroughputProps(bootstrapServers));

        System.out.println("\n=== Summary ===");
        System.out.println("Low-latency profile  : " + lowLatencyMs + " ms for " + recordCount + " records");
        System.out.println("High-throughput profile: " + highThroughputMs + " ms for " + recordCount + " records");
        System.out.println("(Throughput profile usually wins on total time for large batches; " +
                "latency profile wins on per-record delivery time. Run with larger recordCount to see it widen.)");
    }

    private static Properties lowLatencyProps(String bootstrapServers) {
        Properties props = baseProps(bootstrapServers);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384"); // default
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        return props;
    }

    private static Properties highThroughputProps(String bootstrapServers) {
        Properties props = baseProps(bootstrapServers);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "131072"); // 128KB, bigger batches
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864"); // 64MB
        return props;
    }

    private static Properties baseProps(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    private static long runProfile(String bootstrapServers, String topic, int recordCount, Properties props)
            throws ExecutionException, InterruptedException {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long start = System.currentTimeMillis();
            for (int i = 0; i < recordCount; i++) {
                ProducerRecord<String, String> record =
                        new ProducerRecord<>(topic, "key-" + (i % 100), "value-payload-" + i);
                // .get() forces synchronous wait per record in this demo loop so the
                // timing comparison is fair; in real code you'd use the async callback.
                producer.send(record).get();
            }
            producer.flush();
            long elapsed = System.currentTimeMillis() - start;
            System.out.println("Sent " + recordCount + " records in " + elapsed + " ms");
            return elapsed;
        }
    }
}
