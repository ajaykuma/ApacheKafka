package com.kafka.examples.streams.basics;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * SECTION 3.2 – KStream Core Operations
 * =======================================
 * Demonstrates all the fundamental KStream DSL operations on sensor data:
 *
 *   filter()     → keep only readings above threshold
 *   filterNot()  → inverse filter
 *   mapValues()  → transform the value (add derived fields)
 *   map()        → transform key AND value (rekey by location)
 *   flatMap()    → one record → multiple records
 *   branch()     → split stream into multiple sub-streams by predicate
 *   merge()      → combine two streams into one
 *   peek()       → side-effect without changing records (logging, metrics)
 *   selectKey()  → change the partition key
 *
 * Input topic:  streams.sensor.readings  (SensorReading JSON)
 * Output topic: streams.sensor.alerts    (filtered high-temp alerts)
 *
 * HOW TO RUN:
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.basics.KStreamOperations"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.basics.SensorDataProducer"
 */
public class KStreamOperations {

    private static final Logger log = LoggerFactory.getLogger(KStreamOperations.class);

    private static final double HIGH_TEMP_THRESHOLD   = 75.0;
    private static final double CRITICAL_TEMP_THRESHOLD = 90.0;

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_SENSOR_READINGS,
                KafkaConfig.TOPIC_SENSOR_ALERTS
        );

        var streams = new KafkaStreams(buildTopology(), KafkaConfig.streamsProps("kstream-operations-demo"));
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Streams error", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        log.info(" KStream operations topology started.");
        log.info("   Run SensorDataProducer to push sensor readings.");

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        latch.await();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        // Source stream: all sensor readings
        KStream<String, String> allReadings = builder.stream(
                KafkaConfig.TOPIC_SENSOR_READINGS,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── peek() ────────────────────────────────────────────────────────
        // Side-effect logging WITHOUT modifying records.
        // Use for metrics, audit logging, debugging — never for business logic.
        allReadings.peek((sensorId, json) ->
                log.info(" Raw reading received: sensorId={}", sensorId));

        // ── filter() ──────────────────────────────────────────────────────
        // Keep only readings where temperature is above threshold.
        // Records not matching are DROPPED from the stream (not sent to DLT).
        KStream<String, String> hotReadings = allReadings.filter((sensorId, json) -> {
            var reading = JsonUtil.fromJson(json, SensorReading.class);
            return reading.temperature() > HIGH_TEMP_THRESHOLD;
        });

        // ── mapValues() ───────────────────────────────────────────────────
        // Transform the value. Key stays the same (no repartitioning).
        // Use mapValues() instead of map() when you don't need to change the key.
        KStream<String, String> enrichedAlerts = hotReadings.mapValues((sensorId, json) -> {
            var reading = JsonUtil.fromJson(json, SensorReading.class);
            // Add severity level based on temperature
            String severity = reading.temperature() > CRITICAL_TEMP_THRESHOLD ? "CRITICAL" : "WARNING";
            var alert = new SensorAlert(
                    reading.sensorId(), reading.location(),
                    reading.temperature(), severity,
                    "Temperature threshold exceeded: " + reading.temperature() + "°C"
            );
            log.info(" {} alert: sensorId={} temp={}°C location={}",
                    severity, sensorId, reading.temperature(), reading.location());
            return JsonUtil.toJson(alert);
        });

        // ── branch() ──────────────────────────────────────────────────────
        // Split the enriched alerts stream into CRITICAL and WARNING sub-streams.
        // Each record goes to exactly ONE branch (first matching predicate wins).
        Map<String, KStream<String, String>> branches = enrichedAlerts.split(Named.as("severity-"))
                .branch((key, json) -> json.contains("\"CRITICAL\""), Branched.as("critical"))
                .branch((key, json) -> json.contains("\"WARNING\""),  Branched.as("warning"))
                .defaultBranch(Branched.as("unknown"));

        KStream<String, String> criticalAlerts = branches.get("severity-critical");
        KStream<String, String> warningAlerts  = branches.get("severity-warning");

        criticalAlerts.peek((k, v) -> log.info(" CRITICAL ALERT → sensorId={}", k));
        warningAlerts .peek((k, v) -> log.info(" WARNING  ALERT → sensorId={}", k));

        // ── merge() ───────────────────────────────────────────────────────
        // Combine both alert streams back into one for the output topic.
        // In production: critical might go to a pager, warnings to a dashboard.
        criticalAlerts.merge(warningAlerts)
                .to(KafkaConfig.TOPIC_SENSOR_ALERTS,
                        Produced.with(Serdes.String(), Serdes.String()));

        // ── selectKey() + map() example ───────────────────────────────────
        // Rekey a stream by location instead of sensorId.
        // NOTE: selectKey triggers a repartition (network shuffle) — use sparingly.
        allReadings
                .filter((k, json) -> {
                    var r = JsonUtil.fromJson(json, SensorReading.class);
                    return r.humidity() > 80.0; // high humidity readings
                })
                .selectKey((sensorId, json) -> {
                    var r = JsonUtil.fromJson(json, SensorReading.class);
                    return r.location(); // rekey by location
                })
                .peek((location, json) -> {
                    var r = JsonUtil.fromJson(json, SensorReading.class);
                    log.info(" High humidity @ location={} humidity={}%",
                            location, r.humidity());
                })
                // In a real topology, this would go to another output topic
                .to("streams.sensor.humidity-alerts",
                        Produced.with(Serdes.String(), Serdes.String()));

        // ── flatMap() example ─────────────────────────────────────────────
        // One record → multiple output records.
        // Use case: one reading fires alerts for EACH metric that exceeds threshold.
        allReadings.flatMap((sensorId, json) -> {
            var r = JsonUtil.fromJson(json, SensorReading.class);
            var results = new java.util.ArrayList<
                    org.apache.kafka.streams.KeyValue<String, String>>();

            if (r.temperature() > HIGH_TEMP_THRESHOLD) {
                results.add(org.apache.kafka.streams.KeyValue.pair(
                        sensorId + "-TEMP", "TEMP_ALERT:" + r.temperature()));
            }
            if (r.humidity() > 80.0) {
                results.add(org.apache.kafka.streams.KeyValue.pair(
                        sensorId + "-HUM", "HUMIDITY_ALERT:" + r.humidity()));
            }
            return results; // 0, 1, or 2 output records per input
        }).peek((k, v) -> log.info(" flatMap output: key={} val={}", k, v));
        // (not routed to output topic here — demonstrating the operation)

        return builder.build();
    }

    // Alert model produced by this topology
    public record SensorAlert(
            String sensorId,
            String location,
            double temperature,
            String severity,
            String message
    ) {}
}
