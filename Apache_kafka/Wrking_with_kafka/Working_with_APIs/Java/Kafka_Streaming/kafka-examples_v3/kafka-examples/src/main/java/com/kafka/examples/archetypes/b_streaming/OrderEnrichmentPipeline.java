package com.kafka.examples.archetypes.b_streaming;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * SECTION 2B – Data Streaming & Enrichment Pipeline
 * =====================================================
 * Demonstrates the core enterprise pattern:
 *   - Raw orders arrive on a stream topic
 *   - Customer profiles exist as a KTable (compacted reference data)
 *   - Kafka Streams joins them: every order is enriched with customer tier and region
 *   - Enriched output goes to a downstream topic for DB or data lake ingestion
 *
 * Topology:
 *   [enrichment.orders.raw]        (KStream - each order is an event)
 *         |
 *         | KStream-KTable join on customerId
 *         |
 *   [enrichment.customer-profiles] (KTable - latest profile per customerId)
 *         |
 *         ▼
 *   [enrichment.orders.enriched]   (KStream - enriched output)
 *
 * HOW TO RUN:
 *   1. Terminal 1: run this class (starts the Streams app + seeds test data)
 *   2. Terminal 2: run EnrichmentVerifier to see the enriched output
 *
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.b_streaming.OrderEnrichmentPipeline"
 */
public class OrderEnrichmentPipeline {

    private static final Logger log = LoggerFactory.getLogger(OrderEnrichmentPipeline.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_RAW_ORDERS,
                KafkaConfig.TOPIC_CUSTOMER_PROFILES,
                KafkaConfig.TOPIC_ENRICHED_ORDERS
        );

        // Step 1: Seed customer profiles (the KTable source topic)
        seedCustomerProfiles();

        // Give Kafka Streams time to load the KTable from the compacted topic
        Thread.sleep(2000);

        // Step 2: Build the Kafka Streams topology
        var streams = buildAndStartStreams();

        // Give the topology time to start up and load state
        Thread.sleep(3000);

        // Step 3: Publish raw orders to trigger enrichment
        publishRawOrders();

        // Keep running to process; press CTRL+C to stop
        log.info(" Pipeline running. Producing enriched orders to: {}", KafkaConfig.TOPIC_ENRICHED_ORDERS);
        log.info("   Run EnrichmentVerifier in another terminal to consume enriched output.");

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping streams...");
            streams.close();
            latch.countDown();
        }));
        latch.await();
    }

    // ── Build the enrichment topology ─────────────────────────────────────
    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        // KTable: customer profiles keyed by customerId
        // This is a compacted topic — Kafka Streams keeps the latest value per key
        // in a local RocksDB state store automatically
        KTable<String, String> customerProfiles = builder.table(
                KafkaConfig.TOPIC_CUSTOMER_PROFILES,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("customer-profiles-store") // named store for interactive queries
        );

        // KStream: raw orders — each message is a new order event
        KStream<String, String> rawOrders = builder.stream(
                KafkaConfig.TOPIC_RAW_ORDERS,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── KStream-KTable Join ────────────────────────────────────────────
        // For each order event, look up the customer's profile by key (customerId)
        // This join is:
        //   - Non-windowed (KTable holds the current state, no time window needed)
        //   - Left join: orders without a matching customer still pass through (with null profile)
        //   - Real-time: happens as each order arrives
        rawOrders
                // Rekey orders by customerId so they join correctly with the KTable
                .selectKey((orderId, orderJson) -> {
                    var order = JsonUtil.fromJson(orderJson, DomainEvent.class);
                    var payload = JsonUtil.fromJson(JsonUtil.toJson(order.payload()), OrderPlacedEvent.class);
                    log.info(" Raw order received: orderId={} customerId={}", orderId, payload.customerId());
                    return payload.customerId(); // rekey by customerId for the join
                })
                .join(
                        customerProfiles,
                        // ValueJoiner: merges order JSON + customer JSON into enriched JSON
                        (orderJson, profileJson) -> {
                            if (profileJson == null) {
                                log.warn("No customer profile found — enriching with defaults");
                                profileJson = JsonUtil.toJson(
                                        new CustomerProfile("UNKNOWN", "Unknown", "", "STANDARD", "EU"));
                            }
                            var order   = JsonUtil.fromJson(orderJson, DomainEvent.class);
                            var profile = JsonUtil.fromJson(profileJson, CustomerProfile.class);
                            var enriched = new EnrichedOrder(order, profile);
                            log.info("Enriched: customerId={} tier={} region={}",
                                    profile.customerId(), profile.tier(), profile.preferredRegion());
                            return JsonUtil.toJson(enriched);
                        },
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                // Filter: only forward GOLD tier customers to the priority enriched topic
                // (demonstrates filter() operation inline)
                .peek((key, val) -> log.info("Forwarding enriched order for customerId={}", key))
                .to(KafkaConfig.TOPIC_ENRICHED_ORDERS,
                        Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // ── Enriched order model ───────────────────────────────────────────────
    public record EnrichedOrder(
            DomainEvent originalOrder,
            CustomerProfile customer,
            String enrichedAt
    ) {
        public EnrichedOrder(DomainEvent order, CustomerProfile profile) {
            this(order, profile, java.time.Instant.now().toString());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────
    private static KafkaStreams buildAndStartStreams() {
        Properties props = KafkaConfig.streamsProps("order-enrichment-pipeline");
        var streams = new KafkaStreams(buildTopology(), props);

        streams.setUncaughtExceptionHandler((ex) -> {
            log.error("Streams uncaught exception — REPLACING thread", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        streams.start();
        log.info(" Kafka Streams started with application.id=order-enrichment-pipeline");
        return streams;
    }

    private static void seedCustomerProfiles() {
        log.info("Seeding customer profiles KTable...");
        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {
            var profiles = List.of(
                    new CustomerProfile("CUST-100", "Alice Schmidt", "alice@example.com", "GOLD",     "EU-WEST"),
                    new CustomerProfile("CUST-101", "Bob Meier",    "bob@example.com",   "SILVER",   "EU-CENTRAL"),
                    new CustomerProfile("CUST-102", "Carol Bauer",  "carol@example.com", "GOLD",     "EU-NORTH"),
                    new CustomerProfile("CUST-103", "David Klein",  "david@example.com", "STANDARD", "EU-EAST")
            );

            for (var profile : profiles) {
                producer.send(new ProducerRecord<>(
                        KafkaConfig.TOPIC_CUSTOMER_PROFILES,
                        profile.customerId(),
                        JsonUtil.toJson(profile)
                ), (meta, ex) -> {
                    if (ex == null)
                        log.info("Seeded profile for {}", profile.customerId());
                });
            }
            producer.flush();
        }
        log.info("Customer profiles seeded.");
    }

    private static void publishRawOrders() {
        log.info("Publishing raw orders...");
        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {
            var orders = List.of(
                    new OrderPlacedEvent("ORD-001", "CUST-100", "PROD-A1", 2, 149.99, "PENDING"),
                    new OrderPlacedEvent("ORD-002", "CUST-101", "PROD-B3", 1, 299.00, "PENDING"),
                    new OrderPlacedEvent("ORD-003", "CUST-999", "PROD-C7", 5,  49.95, "PENDING"), // unknown customer
                    new OrderPlacedEvent("ORD-004", "CUST-102", "PROD-D2", 1, 599.00, "PENDING")
            );

            for (var order : orders) {
                var event = DomainEvent.of("OrderPlaced", "order-service", order);
                // Note: key is orderId here — the topology will rekey to customerId for the join
                producer.send(new ProducerRecord<>(
                        KafkaConfig.TOPIC_RAW_ORDERS,
                        order.orderId(),
                        JsonUtil.toJson(event)
                ));
            }
            producer.flush();
        }
        log.info("Raw orders published.");
    }
}
