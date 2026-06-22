package com.kafka.examples.streams.statestores;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * SECTION 3.4 – State Stores & Interactive Queries
 * ===================================================
 * Demonstrates:
 *
 *   1. Custom aggregation with a persistent KeyValueStore (RocksDB backed)
 *      — maintains a running stats object per stock ticker
 *
 *   2. Interactive Queries — query a state store from OUTSIDE the topology
 *      (e.g. from a REST endpoint) without going back to Kafka
 *
 *   3. ReadOnlyKeyValueStore — safe read-only view of the store
 *
 * Use case: Real-time stock price statistics per ticker
 *   Input:  streams.stock.prices   (key=ticker, value=StockPrice JSON)
 *   Store:  "stock-stats-store"    (key=ticker, value=StockStats JSON)
 *   Output: streams.stock.alerts   (only when price drops > 5%)
 *
 * HOW TO RUN:
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.statestores.StateStoreExamples"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.statestores.StockPriceProducer"
 *   (the app also queries the store every 5s and prints the contents)
 *
 */
public class StateStoreExamples {

    private static final Logger log = LoggerFactory.getLogger(StateStoreExamples.class);

    static final String STORE_STOCK_STATS = "stock-stats-store";

    // Running statistics maintained in the state store per ticker
    public record StockStats(
            String ticker,
            double latestPrice,
            double minPrice,
            double maxPrice,
            double totalVolume,
            long   tickCount,
            String lastUpdated
    ) {
        // Initial value for the aggregator
        public static StockStats initial(String ticker) {
            return new StockStats(ticker, 0, Double.MAX_VALUE, Double.MIN_VALUE, 0, 0,
                    java.time.Instant.now().toString());
        }

        // Add a new price tick to the stats
        public StockStats update(StockPrice price) {
            return new StockStats(
                    ticker,
                    price.price(),
                    Math.min(minPrice, price.price()),
                    Math.max(maxPrice, price.price()),
                    totalVolume + price.price(), // simplified: price as proxy for volume
                    tickCount + 1,
                    java.time.Instant.now().toString()
            );
        }
    }

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_STOCK_PRICES,
                KafkaConfig.TOPIC_STOCK_ALERTS
        );

        var streams = new KafkaStreams(buildTopology(), KafkaConfig.streamsProps("state-store-examples"));
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Streams error", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();
        log.info(" State store topology started. Run StockPriceProducer in another terminal.");

        // ── Interactive Queries ────────────────────────────────────────────
        // Query the state store directly from outside the topology —
        // no need to read back from a Kafka topic.
        // In production: this would be exposed as a REST endpoint (GET /stats/{ticker})
        Thread.ofVirtual().start(() -> {
            // Wait for streams to be in RUNNING state before querying
            while (streams.state() != KafkaStreams.State.RUNNING) {
                try { Thread.sleep(500); } catch (InterruptedException e) { return; }
            }
            log.info(" Interactive query thread started (polls store every 5s)...");

            while (streams.state() == KafkaStreams.State.RUNNING) {
                try {
                    Thread.sleep(5000);

                    // Get a read-only view of the state store
                    ReadOnlyKeyValueStore<String, String> store = streams.store(
                            StoreQueryParameters.fromNameAndType(
                                    STORE_STOCK_STATS,
                                    QueryableStoreTypes.keyValueStore()
                            )
                    );

                    log.info("=== Interactive Query: Stock Stats Store ===");
                    try (var iterator = store.all()) {
                        if (!iterator.hasNext()) {
                            log.info("  (store is empty — waiting for price data)");
                        }
                        while (iterator.hasNext()) {
                            var entry = iterator.next();
                            var stats = JsonUtil.fromJson(entry.value, StockStats.class);
                            log.info("  {} | latest={} | min={} | max={} | ticks={}",
                                    stats.ticker(),
                                    String.format("%.2f", stats.latestPrice()),
                                    String.format("%.2f", stats.minPrice()),
                                    String.format("%.2f", stats.maxPrice()),
                                    stats.tickCount());
                        }
                    }
                    log.info("============================================");

                } catch (Exception e) {
                    if (streams.state() == KafkaStreams.State.RUNNING) {
                        log.warn("Store query error (may be rebalancing): {}", e.getMessage());
                    }
                }
            }
        });

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { streams.close(); latch.countDown(); }));
        latch.await();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        KStream<String, String> prices = builder.stream(
                KafkaConfig.TOPIC_STOCK_PRICES,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── Custom Aggregation into a persistent State Store ───────────────
        // aggregate() is the most flexible aggregation — you control:
        //   1. initializer()  — creates the initial accumulator value (per key)
        //   2. aggregator()   — how to fold a new record into the accumulator
        //   3. Materialized   — defines the store name, type, and serdes
        //
        // The result is a KTable where each key maps to the latest StockStats.
        KTable<String, String> stockStats = prices
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        // Initializer: called once per new key (first time ticker is seen)
                        () -> JsonUtil.toJson(StockStats.initial("UNKNOWN")),

                        // Aggregator: called for every new price record for this key
                        (ticker, priceJson, currentStatsJson) -> {
                            var price       = JsonUtil.fromJson(priceJson, StockPrice.class);
                            var currentStats = JsonUtil.fromJson(currentStatsJson, StockStats.class);
                            // Fix the initial ticker name (initializer doesn't know the key),
                            // then call update() to fold in the new price tick.
                            var withTicker = new StockStats(
                                    ticker,
                                    currentStats.latestPrice(),
                                    currentStats.minPrice(),
                                    currentStats.maxPrice(),
                                    currentStats.totalVolume(),
                                    currentStats.tickCount(),
                                    currentStats.lastUpdated()
                            );
                            var updated = withTicker.update(price);
                            log.info(" [{}] price={} | min={} | max={} | ticks={}",
                                    ticker,
                                    String.format("%.2f", price.price()),
                                    String.format("%.2f", updated.minPrice()),
                                    String.format("%.2f", updated.maxPrice()),
                                    updated.tickCount());
                            return JsonUtil.toJson(updated);
                        },

                        // Materialized: persist to a named RocksDB store
                        // This is the store we query via Interactive Queries above
                        Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(STORE_STOCK_STATS)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );

        // ── Alert when price drops more than 5% vs previous close ─────────
        prices
                .join(stockStats,
                        (priceJson, statsJson) -> {
                            var price = JsonUtil.fromJson(priceJson, StockPrice.class);
                            if (price.previousClose() > 0) {
                                double changePct = (price.price() - price.previousClose())
                                        / price.previousClose() * 100;
                                if (changePct < -5.0) {
                                    log.warn(" PRICE DROP ALERT: {} dropped {}% (prev={} now={})",
                                            price.ticker(),
                                            String.format("%.2f", changePct),
                                            String.format("%.2f", price.previousClose()),
                                            String.format("%.2f", price.price()));
                                    return String.format(
                                            "{\"alert\":\"PRICE_DROP\",\"ticker\":\"%s\",\"changePct\":%.2f,\"price\":%.2f}",
                                            price.ticker(), changePct, price.price());
                                }
                            }
                            return null;
                        },
                        Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
                )
                .filter((k, v) -> v != null) // only forward actual alerts
                .to(KafkaConfig.TOPIC_STOCK_ALERTS, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
