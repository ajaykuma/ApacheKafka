package com.kafka.examples.streams.joins;

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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * SECTION 3.2 – All Three Join Types
 * =====================================
 * Demonstrates the three Kafka Streams join types with clear real-world use cases:
 *
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │ Join Type           │ When to use                    │ Window?              │
 * ├─────────────────────┼────────────────────────────────┼──────────────────────┤
 * │ KStream-KStream     │ Two event streams, correlate   │ YES (required)       │
 * │                     │ by time (e.g. click + purchase)│                      │
 * ├─────────────────────┼────────────────────────────────┼──────────────────────┤
 * │ KStream-KTable      │ Enrich events with reference   │ NO                   │
 * │                     │ data (latest state per key)    │                      │
 * ├─────────────────────┼────────────────────────────────┼──────────────────────┤
 * │ KStream-GlobalKTable│ Enrich without repartitioning  │ NO                   │
 * │                     │ Small lookup tables            │                      │
 * └─────────────────────┴────────────────────────────────┴──────────────────────┘
 *
 * HOW TO RUN:
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.joins.JoinExamples"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.joins.JoinDataProducer"
 */
public class JoinExamples {

    private static final Logger log = LoggerFactory.getLogger(JoinExamples.class);

    // Topic names specific to this example
    public static final String TOPIC_CLICKS       = "joins.click-events";
    public static final String TOPIC_PURCHASES    = "joins.purchase-events";
    public static final String TOPIC_USER_PREFS   = "joins.user-preferences";
    public static final String TOPIC_REGION_MAP   = "joins.region-map";
    public static final String TOPIC_CLICK_JOINED = "joins.click-purchase-joined";
    public static final String TOPIC_ENRICHED     = "joins.enriched-clicks";

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                TOPIC_CLICKS, TOPIC_PURCHASES,
                TOPIC_USER_PREFS, TOPIC_REGION_MAP,
                TOPIC_CLICK_JOINED, TOPIC_ENRICHED
        );

        var streams = new KafkaStreams(buildTopology(), KafkaConfig.streamsProps("join-examples"));
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Streams error", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        log.info(" Join topology started. Run JoinDataProducer in another terminal.");

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { streams.close(); latch.countDown(); }));
        latch.await();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        // ══════════════════════════════════════════════════════════════════
        // JOIN 1 — KStream-KStream (Windowed Inner Join)
        // ══════════════════════════════════════════════════════════════════
        // Use case: Detect users who clicked on a product AND purchased it
        //           within 5 minutes (attribution / conversion tracking).
        //
        // Both streams must have the SAME KEY (userId) for the join to work.
        // The window ensures we only join events that are close in time.
        KStream<String, String> clickStream = builder.stream(
                TOPIC_CLICKS, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> purchaseStream = builder.stream(
                TOPIC_PURCHASES, Consumed.with(Serdes.String(), Serdes.String()));

        clickStream
                .join(purchaseStream,
                        // ValueJoiner: receives (click, purchase) — combine into conversion event
                        (clickJson, purchaseJson) -> {
                            var result = String.format(
                                    "{\"type\":\"CONVERSION\",\"userId\":\"%s\",\"click\":%s,\"purchase\":%s}",
                                    "matched", clickJson, purchaseJson);
                            log.info(" [KStream-KStream] Conversion detected! click+purchase matched within window");
                            return result;
                        },
                        // Window: join events within 5 minutes of each other
                        JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(5)),
                        StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                .peek((k, v) -> log.info("   → Conversion result: userId={}", k))
                .to(TOPIC_CLICK_JOINED, Produced.with(Serdes.String(), Serdes.String()));

        // ══════════════════════════════════════════════════════════════════
        // JOIN 2 — KStream-KTable (Non-Windowed, Latest State)
        // ══════════════════════════════════════════════════════════════════
        // Use case: Enrich click events with the user's current preferences.
        // The KTable holds the LATEST preference record per userId.
        // No window needed — the table always reflects current state.
        //
        // KEY REQUIREMENT: both stream and table must be partitioned by the
        // same key (userId). If the stream uses a different key, use selectKey()
        // before the join (which triggers a repartition).
        KTable<String, String> userPreferences = builder.table(
                TOPIC_USER_PREFS,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("user-preferences-store")
        );

        clickStream
                // Left join: clicks without a matching user preference still pass through
                .leftJoin(userPreferences,
                        (clickJson, prefsJson) -> {
                            String prefs = prefsJson != null ? prefsJson : "{\"theme\":\"default\"}";
                            var enriched = String.format(
                                    "{\"type\":\"ENRICHED_CLICK\",\"click\":%s,\"preferences\":%s}",
                                    clickJson, prefs);
                            log.info("[KStream-KTable] Click enriched with user preferences");
                            return enriched;
                        },
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                .peek((k, v) -> log.info("   → Enriched click for userId={}", k))
                .to(TOPIC_ENRICHED, Produced.with(Serdes.String(), Serdes.String()));

        // ══════════════════════════════════════════════════════════════════
        // JOIN 3 — KStream-GlobalKTable (No Repartitioning Required)
        // ══════════════════════════════════════════════════════════════════
        // Use case: Enrich clicks with region/country names from a small lookup table.
        //
        // GlobalKTable vs KTable:
        //   KTable      → each partition of the table is on ONE instance
        //                 → join requires the stream to be co-partitioned (same key)
        //   GlobalKTable → FULL copy of the table on EVERY instance
        //                 → join works with ANY key mapping (no repartitioning!)
        //                 → only use for SMALL reference datasets (< a few hundred MB)
        //
        // Here the stream key is userId but we join on the region field inside the value.
        // This is IMPOSSIBLE with KTable (different keys) but POSSIBLE with GlobalKTable.
        GlobalKTable<String, String> regionMap = builder.globalTable(
                TOPIC_REGION_MAP,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.as("region-lookup-store")
        );

        clickStream
                .join(regionMap,
                        // KeyValueMapper: extract the JOIN KEY from the stream record
                        // Here we extract the region code from the click event value
                        (userId, clickJson) -> {
                            // extract region from click JSON (e.g. "EU-WEST")
                            // In real code parse JSON properly; simplified here
                            return clickJson.contains("EU") ? "EU" : "US";
                        },
                        // ValueJoiner: merge click + region data
                        (clickJson, regionJson) -> {
                            var result = String.format(
                                    "{\"type\":\"REGION_ENRICHED\",\"click\":%s,\"region\":%s}",
                                    clickJson, regionJson != null ? regionJson : "{}");
                            log.info(" [KStream-GlobalKTable] Click enriched with region data (no repartition)");
                            return result;
                        }
                )
                .peek((k, v) -> log.info("   → Region-enriched click for userId={}", k));
        // (not written to output topic to keep example clean)

        return builder.build();
    }
}
