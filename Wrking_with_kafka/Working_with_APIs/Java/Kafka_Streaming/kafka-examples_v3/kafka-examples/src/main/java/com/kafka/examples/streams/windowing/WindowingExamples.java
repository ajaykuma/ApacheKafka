package com.kafka.examples.streams.windowing;

import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;

/**
 * SECTION 3.3 – Windowing & Aggregation
 * ========================================
 * All three window types on the same click-event stream:
 *
 * ┌────────────────┬────────────────────────────────────────────────────────┐
 * │ Window Type    │ Behaviour                                              │
 * ├────────────────┼────────────────────────────────────────────────────────┤
 * │ Tumbling       │ Fixed size, non-overlapping. Every event falls into    │
 * │                │ exactly ONE window. Good for hourly/daily aggregations.│
 * ├────────────────┼────────────────────────────────────────────────────────┤
 * │ Hopping        │ Fixed size, overlapping by hop interval. Each event    │
 * │                │ can appear in MULTIPLE windows. Good for rolling stats.│
 * ├────────────────┼────────────────────────────────────────────────────────┤
 * │ Session        │ Activity-based, variable size. Window closes after an  │
 * │                │ inactivity gap. Good for user sessions.                │
 * └────────────────┴────────────────────────────────────────────────────────┘
 *
 * Also demonstrates:
 *   - suppress() to emit only FINAL window results (not every intermediate update)
 *   - count() and aggregate() operations
 *
 * Input: streams.click-events   (key=userId, value=page)
 *
 * HOW TO RUN:
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.windowing.WindowingExamples"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.windowing.ClickEventProducer"
 */
public class WindowingExamples {

    private static final Logger log = LoggerFactory.getLogger(WindowingExamples.class);

    public static final String TOPIC_CLICKS         = "windowing.click-events";
    public static final String TOPIC_TUMBLING_OUT   = "windowing.counts.tumbling";
    public static final String TOPIC_HOPPING_OUT    = "windowing.counts.hopping";
    public static final String TOPIC_SESSION_OUT    = "windowing.counts.session";

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                TOPIC_CLICKS, TOPIC_TUMBLING_OUT, TOPIC_HOPPING_OUT, TOPIC_SESSION_OUT
        );

        var streams = new KafkaStreams(buildTopology(), KafkaConfig.streamsProps("windowing-examples"));
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Streams error", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        log.info(" Windowing topology started. Run ClickEventProducer in another terminal.");

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { streams.close(); latch.countDown(); }));
        latch.await();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        KStream<String, String> clicks = builder.stream(
                TOPIC_CLICKS,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── 1. TUMBLING WINDOW ────────────────────────────────────────────
        // Count clicks per user in fixed 10-second non-overlapping windows.
        // Window:  |---10s---|---10s---|---10s---|
        // Each click lands in exactly one window.
        //
        // suppress(Suppressed.untilWindowCloses): wait until the window is
        // fully closed before emitting — avoids flooding downstream with
        // intermediate partial counts on every new record.
        clicks
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("tumbling-counts-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                // suppress: only emit when the window closes (final result only)
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((windowedKey, count) ->
                        log.info(" [TUMBLING 10s] userId={} window=[{} → {}] clickCount={}",
                                windowedKey.key(),
                                windowedKey.window().startTime(),
                                windowedKey.window().endTime(),
                                count))
                // Map windowed key back to plain string for output topic
                .map((windowedKey, count) ->
                        org.apache.kafka.streams.KeyValue.pair(
                                windowedKey.key(),
                                String.format("{\"window\":\"TUMBLING\",\"userId\":\"%s\",\"count\":%d}",
                                        windowedKey.key(), count)))
                .to(TOPIC_TUMBLING_OUT, Produced.with(Serdes.String(), Serdes.String()));

        // ── 2. HOPPING WINDOW ─────────────────────────────────────────────
        // Count clicks per user in 30-second windows that advance every 10 seconds.
        // Window size=30s, advance=10s → each event appears in 3 windows.
        //
        // Use case: "How many clicks in the last 30 seconds?" updated every 10s.
        // Window:  |----30s----|
        //               |----30s----|
        //                    |----30s----|
        clicks
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30))
                        .advanceBy(Duration.ofSeconds(10)))
                .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("hopping-counts-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .peek((windowedKey, count) ->
                        log.info(" [HOPPING 30s/10s] userId={} count={}", windowedKey.key(), count))
                .map((windowedKey, count) ->
                        org.apache.kafka.streams.KeyValue.pair(
                                windowedKey.key(),
                                String.format("{\"window\":\"HOPPING\",\"userId\":\"%s\",\"count\":%d}",
                                        windowedKey.key(), count)))
                .to(TOPIC_HOPPING_OUT, Produced.with(Serdes.String(), Serdes.String()));

        // ── 3. SESSION WINDOW ─────────────────────────────────────────────
        // Group clicks into user sessions. A session ends when there is no
        // activity for more than 15 seconds (inactivity gap).
        //
        // Use case: track how many pages a user viewed in one browsing session.
        // Window size is VARIABLE — it grows as long as clicks keep arriving.
        // If a user is quiet for 15s, the session closes and a new one starts.
        clicks
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(15)))
                .count(Materialized.<String, Long, org.apache.kafka.streams.state.SessionStore<Bytes, byte[]>>as("session-counts-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                .toStream()
                .filter((windowedKey, count) -> count != null && count > 0) // suppress session deletions
                .peek((windowedKey, count) ->
                        log.info(" [SESSION gap=15s] userId={} sessionStart={} pagesViewed={}",
                                windowedKey.key(),
                                windowedKey.window().startTime(),
                                count))
                .map((windowedKey, count) ->
                        org.apache.kafka.streams.KeyValue.pair(
                                windowedKey.key(),
                                String.format("{\"window\":\"SESSION\",\"userId\":\"%s\",\"pagesViewed\":%d,\"sessionStart\":\"%s\"}",
                                        windowedKey.key(), count, windowedKey.window().startTime())))
                .to(TOPIC_SESSION_OUT, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
