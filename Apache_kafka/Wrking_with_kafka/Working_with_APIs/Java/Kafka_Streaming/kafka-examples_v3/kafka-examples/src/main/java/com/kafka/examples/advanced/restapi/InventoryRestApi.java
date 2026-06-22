package com.kafka.examples.advanced.restapi;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * ADVANCED – Interactive Queries as a REST API
 * ==============================================
 * Exposes a Kafka Streams state store directly over HTTP using Java's
 * built-in HttpServer (no Spring/Quarkus needed — zero extra dependencies).
 *
 * In production this pattern allows microservices to:
 *   - Query current state without a round-trip to Kafka or an external DB
 *   - Build read-your-own-writes APIs backed by Kafka Streams
 *   - Expose materialized views as REST endpoints
 *
 * REST Endpoints:
 *   GET /api/products              → all products in the store
 *   GET /api/products/{productId}  → single product by ID
 *   GET /api/products/low-stock    → products with stock < threshold
 *   GET /api/stats                 → store statistics
 *
 * Topology:
 *   [restapi.inventory.events] → aggregate stock per productId → KeyValueStore
 *                                                              → HTTP REST API
 *
 * HOW TO RUN:
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.restapi.InventoryRestApi"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.restapi.InventoryEventProducer"
 *   Browser/curl: http://localhost:7070/api/products
 */
public class InventoryRestApi {

    private static final Logger log = LoggerFactory.getLogger(InventoryRestApi.class);

    public static final String TOPIC_INVENTORY   = "restapi.inventory.events";
    public static final String STORE_INVENTORY   = "inventory-store";
    public static final int    REST_PORT         = 7070;
    public static final int    LOW_STOCK_THRESHOLD = 20;

    // Inventory state maintained per product in the store
    public record ProductInventory(
            String productId,
            String productName,
            int    currentStock,
            int    totalIn,
            int    totalOut,
            int    eventCount,
            String lastUpdated
    ) {
        public ProductInventory apply(WarehouseEvent event) {
            int newStock = switch (event.action()) {
                case "STOCK_IN"    -> currentStock + event.quantity();
                case "STOCK_OUT"   -> Math.max(0, currentStock - event.quantity());
                case "ADJUSTMENT"  -> event.quantity(); // absolute set
                default            -> currentStock;
            };
            int newIn  = event.action().equals("STOCK_IN")  ? totalIn  + event.quantity() : totalIn;
            int newOut = event.action().equals("STOCK_OUT") ? totalOut + event.quantity() : totalOut;
            return new ProductInventory(
                    productId, productName, newStock, newIn, newOut,
                    eventCount + 1, java.time.Instant.now().toString());
        }
    }

    private static KafkaStreams streams;

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(TOPIC_INVENTORY);

        streams = new KafkaStreams(buildTopology(), KafkaConfig.streamsProps("inventory-rest-api"));
        streams.setUncaughtExceptionHandler(ex -> {
            log.error("Streams error", ex);
            return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler
                    .StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        streams.start();

        // Wait for streams to be RUNNING before starting REST server
        while (streams.state() != KafkaStreams.State.RUNNING) {
            Thread.sleep(200);
        }
        log.info(" Kafka Streams RUNNING");

        // Start embedded HTTP server
        startRestServer();

        log.info(" REST API started on http://localhost:{}", REST_PORT);
        log.info("   Endpoints:");
        log.info("   GET http://localhost:{}/api/products", REST_PORT);
        log.info("   GET http://localhost:{}/api/products/PROD-A1", REST_PORT);
        log.info("   GET http://localhost:{}/api/products/low-stock", REST_PORT);
        log.info("   GET http://localhost:{}/api/stats", REST_PORT);
        log.info("   Run InventoryEventProducer to populate the store.");

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            streams.close();
            latch.countDown();
        }));
        latch.await();
    }

    // ── Kafka Streams Topology ─────────────────────────────────────────────
    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        builder.stream(TOPIC_INVENTORY,
                        Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        // Initializer
                        () -> JsonUtil.toJson(new ProductInventory(
                                "UNKNOWN", "Unknown Product", 0, 0, 0, 0,
                                java.time.Instant.now().toString())),

                        // Aggregator: fold each warehouse event into running inventory
                        (productId, eventJson, currentJson) -> {
                            var event   = JsonUtil.fromJson(eventJson, WarehouseEvent.class);
                            var current = JsonUtil.fromJson(currentJson, ProductInventory.class);

                            // Fix productId and name on first event (initializer doesn't know the key)
                            var withId = new ProductInventory(
                                    productId,
                                    getProductName(productId),
                                    current.currentStock(),
                                    current.totalIn(),
                                    current.totalOut(),
                                    current.eventCount(),
                                    current.lastUpdated()
                            );
                            var updated = withId.apply(event);
                            log.info(" [{}] {} qty={} → stock={}",
                                    productId, event.action(), event.quantity(), updated.currentStock());
                            return JsonUtil.toJson(updated);
                        },

                        Materialized.<String, String, KeyValueStore<
                                org.apache.kafka.common.utils.Bytes, byte[]>>as(STORE_INVENTORY)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );

        return builder.build();
    }

    // ── REST Server ────────────────────────────────────────────────────────
    private static void startRestServer() throws IOException {
        var server = HttpServer.create(new InetSocketAddress(REST_PORT), 0);

        // GET /api/products — all products
        server.createContext("/api/products", exchange -> {
            String path = exchange.getRequestURI().getPath();

            try {
                String response;
                int status = 200;

                if (path.equals("/api/products/low-stock")) {
                    // GET /api/products/low-stock
                    response = getLowStockProducts();

                } else if (path.startsWith("/api/products/")) {
                    // GET /api/products/{productId}
                    String productId = path.substring("/api/products/".length());
                    response = getProduct(productId);
                    if (response == null) {
                        response = String.format("{\"error\":\"Product not found: %s\"}", productId);
                        status = 404;
                    }

                } else {
                    // GET /api/products — all
                    response = getAllProducts();
                }

                sendResponse(exchange, status, response);

            } catch (Exception e) {
                log.error("REST error", e);
                sendResponse(exchange, 500,
                        String.format("{\"error\":\"%s\"}", e.getMessage()));
            }
        });

        // GET /api/stats
        server.createContext("/api/stats", exchange -> {
            try {
                sendResponse(exchange, 200, getStoreStats());
            } catch (Exception e) {
                sendResponse(exchange, 500, "{\"error\":\"" + e.getMessage() + "\"}");
            }
        });

        server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        server.start();
    }

    // ── State Store Query Methods ──────────────────────────────────────────
    private static ReadOnlyKeyValueStore<String, String> getStore() {
        return streams.store(StoreQueryParameters.fromNameAndType(
                STORE_INVENTORY, QueryableStoreTypes.keyValueStore()));
    }

    private static String getAllProducts() {
        var store = getStore();
        var results = new ArrayList<String>();
        try (var it = store.all()) {
            while (it.hasNext()) results.add(it.next().value);
        }
        return "[" + String.join(",", results) + "]";
    }

    private static String getProduct(String productId) {
        var value = getStore().get(productId);
        return value; // null if not found
    }

    private static String getLowStockProducts() {
        var store = getStore();
        var results = new ArrayList<String>();
        try (var it = store.all()) {
            while (it.hasNext()) {
                var entry = it.next();
                var inv = JsonUtil.fromJson(entry.value, ProductInventory.class);
                if (inv.currentStock() < LOW_STOCK_THRESHOLD) {
                    results.add(entry.value);
                }
            }
        }
        return String.format("{\"threshold\":%d,\"count\":%d,\"products\":[%s]}",
                LOW_STOCK_THRESHOLD, results.size(), String.join(",", results));
    }

    private static String getStoreStats() {
        var store = getStore();
        long count = store.approximateNumEntries();
        int totalStock = 0;
        try (var it = store.all()) {
            while (it.hasNext()) {
                var inv = JsonUtil.fromJson(it.next().value, ProductInventory.class);
                totalStock += inv.currentStock();
            }
        }
        return String.format(
                "{\"streamsState\":\"%s\",\"productCount\":%d,\"totalStockUnits\":%d," +
                "\"lowStockThreshold\":%d,\"timestamp\":\"%s\"}",
                streams.state(), count, totalStock, LOW_STOCK_THRESHOLD,
                java.time.Instant.now());
    }

    private static void sendResponse(
            com.sun.net.httpserver.HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var os = exchange.getResponseBody()) { os.write(bytes); }
    }

    // Simple product name lookup
    private static String getProductName(String productId) {
        return switch (productId) {
            case "PROD-A1" -> "Laptop Stand";
            case "PROD-B3" -> "Wireless Keyboard";
            case "PROD-C7" -> "USB-C Hub";
            case "PROD-D2" -> "Monitor Arm";
            case "PROD-E9" -> "Desk Lamp";
            default        -> productId;
        };
    }
}
