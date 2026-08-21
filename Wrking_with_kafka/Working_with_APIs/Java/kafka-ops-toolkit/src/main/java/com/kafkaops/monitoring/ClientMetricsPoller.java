package com.kafkaops.monitoring;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Polls a consumer's own client-side metrics (records-lag-max, fetch latency,
 * bytes-consumed-rate, etc.) the same way you'd wire up a JMX exporter or
 * Prometheus scrape in production, but printed to stdout for learning/testing.
 *
 * In production these metrics are normally exposed via:
 *  - JMX -> Prometheus JMX exporter -> Grafana dashboard
 *  - Or pushed via a metrics reporter plugin (Kafka supports pluggable
 *    org.apache.kafka.common.metrics.MetricsReporter implementations)
 *
 * This class shows what's available client-side without any extra infra,
 * so you can see exactly which numbers a real dashboard would be built on.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.monitoring.ClientMetricsPoller \
 *       -Dexec.args="localhost:9092 my-topic my-group"
 */
public class ClientMetricsPoller {

    // Metrics worth watching first when diagnosing a "slow consumer" complaint
    private static final java.util.Set<String> KEY_METRICS = java.util.Set.of(
            "records-lag-max",
            "records-consumed-rate",
            "bytes-consumed-rate",
            "fetch-latency-avg",
            "fetch-latency-max",
            "rebalance-latency-avg",
            "commit-latency-avg"
    );

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 3) {
            System.out.println("Usage: ClientMetricsPoller <bootstrap-servers> <topic> <group-id>");
            return;
        }
        String bootstrapServers = args[0];
        String topic = args[1];
        String groupId = args[2];

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.List.of(topic));

            System.out.println("Polling " + topic + " and printing key metrics every poll cycle. Ctrl+C to stop.");
            for (int cycle = 0; cycle < 20; cycle++) {
                consumer.poll(Duration.ofMillis(1000));
                printKeyMetrics(consumer);
                Thread.sleep(2000);
            }
        }
    }

    /* old approach
    private static void printKeyMetrics(KafkaConsumer<String, String> consumer) {
        Map<MetricName, ? extends Metric> metrics = consumer.metrics();

        String snapshot = metrics.entrySet().stream()
                .filter(e -> KEY_METRICS.contains(e.getKey().name()))
                .collect(Collectors.toMap(
                        e -> e.getKey().name(),
                        e -> e.getValue().metricValue(),
                        (a, b) -> a, // keep first on name collision across groups
                        java.util.TreeMap::new))
                .entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", "));

        System.out.println("[" + java.time.LocalTime.now().withNano(0) + "] " + snapshot);
    }
    */
    private static void printKeyMetrics(KafkaConsumer<String, String> consumer) {
        Map<MetricName, ? extends Metric> metrics = consumer.metrics();

        // IMPORTANT: Kafka exposes the same metric name (e.g. "records-lag-max")
        // at multiple scopes simultaneously - client-level (overall), per-topic,
        // and per-partition - distinguished only by tags, not by name. Filtering
        // on name alone and discarding tags causes random collisions between
        // these scopes. We want the client-level aggregate here, so we explicitly
        // exclude any metric tagged with "topic" or "partition".
        String snapshot = metrics.entrySet().stream()
                .filter(e -> KEY_METRICS.contains(e.getKey().name()))
                .filter(e -> !e.getKey().tags().containsKey("topic")
                        && !e.getKey().tags().containsKey("partition"))
                .collect(Collectors.toMap(
                        e -> e.getKey().name(),
                        e -> e.getValue().metricValue(),
                        (a, b) -> a,
                        java.util.TreeMap::new))
                .entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(", "));

        System.out.println("[" + java.time.LocalTime.now().withNano(0) + "] " + snapshot);
    }
}
