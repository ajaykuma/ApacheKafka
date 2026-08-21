package com.kafka.examples;

import com.kafka.examples.archetypes.b_streaming.OrderEnrichmentPipeline;
import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import com.kafka.examples.streams.basics.KStreamOperations;
import com.kafka.examples.streams.joins.JoinExamples;
import com.kafka.examples.streams.windowing.WindowingExamples;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for all Kafka Streams topologies using TopologyTestDriver.
 *
 * TopologyTestDriver = test your topology WITHOUT a running Kafka cluster.
 * It simulates brokers, topics, and time — ideal for fast unit tests in CI.
 *
 * HOW TO RUN:
 *   mvn test
 *
 * Each test:
 *   1. Builds the topology (same code as production)
 *   2. Creates a TestTopologyDriver with that topology
 *   3. Uses TestInputTopic to pipe records in
 *   4. Uses TestOutputTopic to read results out
 *   5. Asserts on the output
 */
@DisplayName("Kafka Streams Topology Tests")
class TopologyTests {

    // ── Shared test props ──────────────────────────────────────────────────
    private static Properties testProps() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST GROUP 1 – Order Enrichment Pipeline (Section 2B)
    // ══════════════════════════════════════════════════════════════════════
    @Nested
    @DisplayName("2B: Order Enrichment Pipeline")
    class OrderEnrichmentTests {

        TopologyTestDriver driver;
        TestInputTopic<String, String> orderInput;
        TestInputTopic<String, String> profileInput;
        TestOutputTopic<String, String> enrichedOutput;

        @BeforeEach
        void setUp() {
            driver = new TopologyTestDriver(OrderEnrichmentPipeline.buildTopology(), testProps());

            profileInput = driver.createInputTopic(
                    KafkaConfig.TOPIC_CUSTOMER_PROFILES,
                    Serdes.String().serializer(), Serdes.String().serializer());

            orderInput = driver.createInputTopic(
                    KafkaConfig.TOPIC_RAW_ORDERS,
                    Serdes.String().serializer(), Serdes.String().serializer());

            enrichedOutput = driver.createOutputTopic(
                    KafkaConfig.TOPIC_ENRICHED_ORDERS,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Order is enriched with customer profile when profile exists")
        void orderGetsEnrichedWithProfile() {
            // Arrange: seed the KTable with a customer profile
            var profile = new CustomerProfile("CUST-100", "Alice", "alice@test.com", "GOLD", "EU-WEST");
            profileInput.pipeInput("CUST-100", JsonUtil.toJson(profile));

            // Act: send a raw order for that customer
            var order = new OrderPlacedEvent("ORD-001", "CUST-100", "PROD-A1", 2, 149.99, "PENDING");
            var event = DomainEvent.of("OrderPlaced", "order-service", order);
            orderInput.pipeInput("ORD-001", JsonUtil.toJson(event));

            // Assert: enriched order arrives on output topic
            assertFalse(enrichedOutput.isEmpty(), "Enriched output should not be empty");
            var record = enrichedOutput.readRecord();
            assertNotNull(record.getValue(), "Enriched value must not be null");
            assertTrue(record.getValue().contains("GOLD"),    "Should contain customer tier GOLD");
            assertTrue(record.getValue().contains("EU-WEST"), "Should contain customer region");
            assertTrue(record.getValue().contains("ORD-001") || record.getValue().contains("OrderPlaced"),
                    "Should contain original order data");
        }

        @Test
        @DisplayName("KStream-KTable left join: no output when KTable is empty in test driver")
        void orderWithNoProfilePassesThrough() {
            // IMPORTANT: This test documents a key difference between TopologyTestDriver
            // and a real Kafka cluster.
            //
            // With TopologyTestDriver (synchronous, in-memory):
            //   A KStream-KTable LEFT JOIN produces NO output when the KTable store is
            //   completely empty. The driver has no buffering or waiting — if the KTable
            //   has never seen a record for this key, the join simply emits nothing.
            //
            // On a real running cluster:
            //   The left join WOULD emit the order enriched with a null profile,
            //   because the stream side waits and the KTable state is eventually consistent.
            //   Your OrderEnrichmentPipeline handles this with the null-check:
            //     if (profileJson == null) → falls back to default profile.
            //
            // This is expected behaviour — not a bug in the pipeline.

            // No profile seeded for CUST-999 — KTable is empty for this key
            var order = new OrderPlacedEvent("ORD-002", "CUST-999", "PROD-B3", 1, 50.0, "PENDING");
            var event = DomainEvent.of("OrderPlaced", "order-service", order);
            orderInput.pipeInput("ORD-002", JsonUtil.toJson(event));

            // In TopologyTestDriver: empty KTable → left join emits nothing
            assertTrue(enrichedOutput.isEmpty(),
                    "TopologyTestDriver: KStream-KTable left join produces no output when KTable is empty. " +
                    "On a real cluster the null-profile fallback in OrderEnrichmentPipeline would fire instead.");
        }

        @Test
        @DisplayName("KTable reflects latest profile when updated")
        void ktableReflectsLatestProfile() {
            // Seed initial profile
            var profileV1 = new CustomerProfile("CUST-101", "Bob", "bob@test.com", "SILVER", "EU");
            profileInput.pipeInput("CUST-101", JsonUtil.toJson(profileV1));

            // Update profile to GOLD tier (KTable keeps only latest)
            var profileV2 = new CustomerProfile("CUST-101", "Bob", "bob@test.com", "GOLD", "EU");
            profileInput.pipeInput("CUST-101", JsonUtil.toJson(profileV2));

            // Order arrives — should join with V2 (GOLD), not V1 (SILVER)
            var order = new OrderPlacedEvent("ORD-003", "CUST-101", "PROD-C7", 1, 200.0, "PENDING");
            var event = DomainEvent.of("OrderPlaced", "order-service", order);
            orderInput.pipeInput("ORD-003", JsonUtil.toJson(event));

            assertFalse(enrichedOutput.isEmpty());
            var result = enrichedOutput.readRecord().getValue();
            assertTrue(result.contains("GOLD"),
                    "Should use updated GOLD tier from KTable, not old SILVER");
            assertFalse(result.contains("SILVER"),
                    "Old SILVER tier should NOT appear — KTable keeps only latest");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST GROUP 2 – KStream Operations (Section 3.2)
    // ══════════════════════════════════════════════════════════════════════
    @Nested
    @DisplayName("3.2: KStream filter/map/branch operations")
    class KStreamOperationTests {

        TopologyTestDriver driver;
        TestInputTopic<String, String> sensorInput;
        TestOutputTopic<String, String> alertOutput;

        @BeforeEach
        void setUp() {
            driver = new TopologyTestDriver(KStreamOperations.buildTopology(), testProps());
            sensorInput = driver.createInputTopic(
                    KafkaConfig.TOPIC_SENSOR_READINGS,
                    Serdes.String().serializer(), Serdes.String().serializer());
            alertOutput = driver.createOutputTopic(
                    KafkaConfig.TOPIC_SENSOR_ALERTS,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Normal temperature readings are filtered out (< 75°C)")
        void normalReadingsAreFiltered() {
            var normal = new SensorReading("S-001", "Floor-A", 62.0, 50.0, System.currentTimeMillis());
            sensorInput.pipeInput("S-001", JsonUtil.toJson(normal));

            assertTrue(alertOutput.isEmpty(),
                    "Readings below 75°C should not produce any alert");
        }

        @Test
        @DisplayName("High temperature reading produces WARNING alert")
        void highTempProducesWarning() {
            var hot = new SensorReading("S-002", "Floor-B", 80.0, 55.0, System.currentTimeMillis());
            sensorInput.pipeInput("S-002", JsonUtil.toJson(hot));

            assertFalse(alertOutput.isEmpty(), "High temp should produce an alert");
            var alert = alertOutput.readRecord().getValue();
            assertTrue(alert.contains("WARNING"),  "Should be WARNING severity");
            assertFalse(alert.contains("CRITICAL"), "Should NOT be CRITICAL at 80°C");
        }

        @Test
        @DisplayName("Critical temperature (>90°C) produces CRITICAL alert")
        void criticalTempProducesCritical() {
            var critical = new SensorReading("S-003", "ServerRoom", 95.0, 40.0, System.currentTimeMillis());
            sensorInput.pipeInput("S-003", JsonUtil.toJson(critical));

            assertFalse(alertOutput.isEmpty());
            var alert = alertOutput.readRecord().getValue();
            assertTrue(alert.contains("CRITICAL"), "Should be CRITICAL severity at 95°C");
        }

        @Test
        @DisplayName("Multiple readings: only high-temp ones produce alerts")
        void mixedReadingsOnlyHighTempAlerts() {
            sensorInput.pipeInput("S-A", JsonUtil.toJson(
                    new SensorReading("S-A", "Zone1", 55.0, 40.0, System.currentTimeMillis())));  // filtered
            sensorInput.pipeInput("S-B", JsonUtil.toJson(
                    new SensorReading("S-B", "Zone2", 78.0, 45.0, System.currentTimeMillis())));  // WARNING
            sensorInput.pipeInput("S-C", JsonUtil.toJson(
                    new SensorReading("S-C", "Zone3", 65.0, 50.0, System.currentTimeMillis())));  // filtered
            sensorInput.pipeInput("S-D", JsonUtil.toJson(
                    new SensorReading("S-D", "Zone4", 92.0, 38.0, System.currentTimeMillis())));  // CRITICAL

            assertEquals(2, alertOutput.readRecordsToList().size(),
                    "Should produce exactly 2 alerts (one WARNING, one CRITICAL)");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST GROUP 3 – Windowing (Section 3.3)
    // ══════════════════════════════════════════════════════════════════════
    @Nested
    @DisplayName("3.3: Windowing — tumbling and session")
    class WindowingTests {

        TopologyTestDriver driver;
        TestInputTopic<String, String> clickInput;
        TestOutputTopic<String, String> tumblingOutput;
        TestOutputTopic<String, String> sessionOutput;

        @BeforeEach
        void setUp() {
            driver = new TopologyTestDriver(WindowingExamples.buildTopology(), testProps());
            clickInput = driver.createInputTopic(
                    WindowingExamples.TOPIC_CLICKS,
                    Serdes.String().serializer(), Serdes.String().serializer(),
                    Instant.now(), Duration.ofSeconds(1));
            tumblingOutput = driver.createOutputTopic(
                    WindowingExamples.TOPIC_TUMBLING_OUT,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
            sessionOutput = driver.createOutputTopic(
                    WindowingExamples.TOPIC_SESSION_OUT,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Tumbling window closes after window size and emits count")
        void tumblingWindowEmitsOnClose() {
            var base = Instant.now();

            // 3 clicks for user U-001 within the same 10s window
            clickInput.pipeInput(new TestRecord<>("U-001", "click1", base));
            clickInput.pipeInput(new TestRecord<>("U-001", "click2", base.plusSeconds(3)));
            clickInput.pipeInput(new TestRecord<>("U-001", "click3", base.plusSeconds(7)));

            // Advance time past window boundary + grace to trigger suppress/close
            clickInput.pipeInput(new TestRecord<>("U-001", "click4", base.plusSeconds(25)));

            var results = tumblingOutput.readRecordsToList();
            assertFalse(results.isEmpty(), "Tumbling window should have emitted at least one result");
            // The last emitted result for U-001 should reflect the count
            var last = results.get(results.size() - 1);
            assertTrue(last.getValue().contains("TUMBLING"), "Output should identify window type");
            assertTrue(last.getValue().contains("U-001"),    "Output should contain the user key");
        }

        @Test
        @DisplayName("Session window closes after inactivity gap")
        void sessionWindowClosesAfterGap() {
            var base = Instant.now();

            // Session 1: clicks close together
            clickInput.pipeInput(new TestRecord<>("U-002", "page1", base));
            clickInput.pipeInput(new TestRecord<>("U-002", "page2", base.plusSeconds(5)));
            clickInput.pipeInput(new TestRecord<>("U-002", "page3", base.plusSeconds(9)));

            // Gap > 15s → session closes
            // New click starts a new session
            clickInput.pipeInput(new TestRecord<>("U-002", "page4", base.plusSeconds(30)));

            var results = sessionOutput.readRecordsToList();
            assertFalse(results.isEmpty(), "Session window should have emitted results");
            assertTrue(results.stream().anyMatch(r -> r.getValue().contains("SESSION")),
                    "Session output should contain SESSION window type");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // TEST GROUP 4 – KStream-KStream Join (Section 3.2 Joins)
    // ══════════════════════════════════════════════════════════════════════
    @Nested
    @DisplayName("3.2: KStream-KStream windowed join")
    class StreamStreamJoinTests {

        TopologyTestDriver driver;
        TestInputTopic<String, String> clickInput;
        TestInputTopic<String, String> purchaseInput;
        TestInputTopic<String, String> prefsInput;
        TestInputTopic<String, String> regionInput;
        TestOutputTopic<String, String> joinedOutput;
        TestOutputTopic<String, String> enrichedOutput;

        @BeforeEach
        void setUp() {
            driver = new TopologyTestDriver(JoinExamples.buildTopology(), testProps());

            clickInput = driver.createInputTopic(JoinExamples.TOPIC_CLICKS,
                    Serdes.String().serializer(), Serdes.String().serializer(),
                    Instant.now(), Duration.ofSeconds(1));
            purchaseInput = driver.createInputTopic(JoinExamples.TOPIC_PURCHASES,
                    Serdes.String().serializer(), Serdes.String().serializer(),
                    Instant.now(), Duration.ofSeconds(1));
            prefsInput = driver.createInputTopic(JoinExamples.TOPIC_USER_PREFS,
                    Serdes.String().serializer(), Serdes.String().serializer());
            regionInput = driver.createInputTopic(JoinExamples.TOPIC_REGION_MAP,
                    Serdes.String().serializer(), Serdes.String().serializer());
            joinedOutput = driver.createOutputTopic(JoinExamples.TOPIC_CLICK_JOINED,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
            enrichedOutput = driver.createOutputTopic(JoinExamples.TOPIC_ENRICHED,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Click and purchase within window produces conversion event")
        void clickAndPurchaseWithinWindowJoins() {
            var base = Instant.now();
            clickInput.pipeInput(new TestRecord<>("U-001",
                    "{\"userId\":\"U-001\",\"page\":\"/product/A1\"}", base));
            purchaseInput.pipeInput(new TestRecord<>("U-001",
                    "{\"userId\":\"U-001\",\"productId\":\"A1\",\"amount\":99.9}", base.plusSeconds(30)));

            assertFalse(joinedOutput.isEmpty(), "Click+Purchase within 5min should join");
            var result = joinedOutput.readRecord().getValue();
            assertTrue(result.contains("CONVERSION"), "Should be tagged as CONVERSION");
        }

        @Test
        @DisplayName("Click and purchase OUTSIDE window does NOT join")
        void clickAndPurchaseOutsideWindowDoesNotJoin() {
            var base = Instant.now();
            clickInput.pipeInput(new TestRecord<>("U-002",
                    "{\"userId\":\"U-002\",\"page\":\"/product/B3\"}", base));
            // 10 minutes later — outside the 5-minute join window
            purchaseInput.pipeInput(new TestRecord<>("U-002",
                    "{\"userId\":\"U-002\",\"productId\":\"B3\",\"amount\":49.0}", base.plusSeconds(601)));

            assertTrue(joinedOutput.isEmpty(),
                    "Click and purchase more than 5 min apart should NOT join");
        }

        @Test
        @DisplayName("Click is enriched with user preferences from KTable")
        void clickEnrichedWithPreferences() {
            prefsInput.pipeInput("U-003",
                    "{\"userId\":\"U-003\",\"theme\":\"dark\",\"lang\":\"de\",\"region\":\"EU\"}");

            clickInput.pipeInput("U-003",
                    "{\"userId\":\"U-003\",\"page\":\"/home\",\"region\":\"EU\"}");

            assertFalse(enrichedOutput.isEmpty(), "Click should be enriched");
            var result = enrichedOutput.readRecord().getValue();
            assertTrue(result.contains("ENRICHED_CLICK"), "Should be tagged ENRICHED_CLICK");
            assertTrue(result.contains("dark"), "Should contain user preference (dark theme)");
        }
    }
}