package com.kafka.examples.advanced.errorhandling;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.errors.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * ADVANCED – Kafka Streams Error Handling
 * =========================================
 * Demonstrates all three error handling strategies inside a Kafka Streams topology:
 *
 * ┌─────────────────────────────────────────────────────────────────────────────┐
 * │ Error Type          │ Where it occurs       │ Strategy demonstrated        │
 * ├─────────────────────┼───────────────────────┼──────────────────────────────┤
 * │ Deserialization     │ Reading from topic    │ SKIP (log + continue)        │
 * │ Processing          │ Inside map/filter     │ DLT (route bad records out)  │
 * │ Production          │ Writing to topic      │ FAIL / CONTINUE              │
 * │ Uncaught exception  │ Stream thread crash   │ REPLACE_THREAD (auto-heal)   │
 * └─────────────────────┴───────────────────────┴──────────────────────────────┘
 *
 * Topology:
 *   [errorhandling.orders.input]
 *         │
 *         ├─ valid records   → transform → [errorhandling.orders.output]
 *         └─ invalid records → route    → [errorhandling.orders.DLT]
 *
 * HOW TO RUN:
 *   Terminal 1: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.errorhandling.StreamsErrorHandling"
 *   Terminal 2: mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.errorhandling.ErrorProducer"
 */
public class StreamsErrorHandling {

    private static final Logger log = LoggerFactory.getLogger(StreamsErrorHandling.class);

    public static final String TOPIC_INPUT  = "errorhandling.orders.input";
    public static final String TOPIC_OUTPUT = "errorhandling.orders.output";
    public static final String TOPIC_DLT    = "errorhandling.orders.DLT";

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(TOPIC_INPUT, TOPIC_OUTPUT, TOPIC_DLT);

        var streams = new KafkaStreams(buildTopology(), buildStreamsProps());

        // ── Strategy 3: Uncaught Exception Handler ─────────────────────────
        // Called when a stream thread crashes with an unhandled exception.
        // REPLACE_THREAD: Kafka Streams kills the crashed thread and starts a
        // new one automatically — the application heals itself.
        // Alternative: SHUTDOWN_CLIENT (stop this instance), SHUTDOWN_APPLICATION (stop all)
        streams.setUncaughtExceptionHandler(ex -> {
            log.error(" Stream thread crashed: {}", ex.getMessage());
            log.info(" Replacing crashed thread automatically (REPLACE_THREAD)...");
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });

        // ── State change listener — useful for monitoring ──────────────────
        streams.setStateListener((newState, oldState) ->
                log.info("🔄 Streams state: {} → {}", oldState, newState));

        streams.start();
        log.info(" Error handling topology started. Run ErrorProducer in another terminal.");

        var latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));
        latch.await();
    }

    public static Topology buildTopology() {
        var builder = new StreamsBuilder();

        KStream<String, String> input = builder.stream(
                TOPIC_INPUT,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // ── Strategy 2: Processing Error → DLT routing ────────────────────
        // Use branch() to separate valid from invalid records BEFORE processing.
        // This is safer than try/catch inside map() because it gives you
        // explicit control over what goes to DLT vs what gets processed.
        //
        // Rule: if we can't parse it OR business validation fails → DLT
        //       if it's valid → process normally
        var branches = input.split(Named.as("validation-"))
                .branch(
                        // Valid: parseable JSON with required fields and positive amount
                        (key, value) -> isValidOrder(key, value),
                        Branched.as("valid")
                )
                .defaultBranch(Branched.as("invalid"));

        KStream<String, String> validOrders   = branches.get("validation-valid");
        KStream<String, String> invalidOrders = branches.get("validation-invalid");

        // ── Process valid orders ───────────────────────────────────────────
        validOrders
                .mapValues((key, value) -> {
                    var order = JsonUtil.fromJson(value, OrderPlacedEvent.class);
                    log.info(" [VALID] Processing orderId={} amount={}", key, order.totalAmount());

                    // Apply business logic — e.g. calculate VAT
                    double vatAmount = order.totalAmount() * 0.19;
                    return String.format(
                            "{\"orderId\":\"%s\",\"originalAmount\":%.2f,\"vatAmount\":%.2f," +
                            "\"totalWithVat\":%.2f,\"status\":\"PROCESSED\"}",
                            order.orderId(), order.totalAmount(), vatAmount,
                            order.totalAmount() + vatAmount);
                })
                .peek((key, value) ->
                        log.info(" [OUTPUT] orderId={} → {}", key, value))
                .to(TOPIC_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));

        // ── Route invalid orders to DLT ────────────────────────────────────
        // Enrich with failure reason before sending to DLT so ops team
        // knows WHY it failed without re-parsing the original record
        invalidOrders
                .mapValues((key, value) -> {
                    String reason = getDltReason(key, value);
                    log.warn(" [DLT] orderId={} reason={}", key, reason);
                    return String.format(
                            "{\"originalKey\":\"%s\",\"reason\":\"%s\"," +
                            "\"originalValue\":%s,\"failedAt\":\"%s\"}",
                            key, reason,
                            value.startsWith("{") ? value : "\"" + value + "\"",
                            java.time.Instant.now());
                })
                .to(TOPIC_DLT, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    // ── Validation logic ───────────────────────────────────────────────────
    private static boolean isValidOrder(String key, String value) {
        try {
            if (value == null || value.isBlank()) return false;
            if (!value.trim().startsWith("{"))    return false; // not JSON
            var order = JsonUtil.fromJson(value, OrderPlacedEvent.class);
            if (order.orderId() == null || order.orderId().isBlank()) return false;
            if (order.totalAmount() <= 0)  return false; // negative/zero amount
            if (order.quantity()   <= 0)   return false; // negative/zero quantity
            if (order.customerId() == null) return false;
            return true;
        } catch (Exception e) {
            return false; // unparseable JSON
        }
    }

    private static String getDltReason(String key, String value) {
        if (value == null || value.isBlank())     return "NULL_OR_EMPTY_VALUE";
        if (!value.trim().startsWith("{"))        return "NOT_JSON";
        try {
            var order = JsonUtil.fromJson(value, OrderPlacedEvent.class);
            if (order.orderId() == null)           return "MISSING_ORDER_ID";
            if (order.totalAmount() <= 0)          return "INVALID_AMOUNT_" + order.totalAmount();
            if (order.quantity() <= 0)             return "INVALID_QUANTITY_" + order.quantity();
            if (order.customerId() == null)        return "MISSING_CUSTOMER_ID";
            return "UNKNOWN_VALIDATION_FAILURE";
        } catch (Exception e) {
            return "PARSE_ERROR: " + e.getMessage().replace("\"", "'");
        }
    }

    // ── Streams config with error handling settings ────────────────────────
    public static Properties buildStreamsProps() {
        var props = KafkaConfig.streamsProps("streams-error-handling");

        // ── Strategy 1: Deserialization Error Handler ──────────────────────
        // Called when a record cannot be deserialized from the topic.
        // LOG_AND_CONTINUE: log the bad record and skip it — don't crash.
        // FAIL_ON_INVALID_TIMESTAMP: alternative — crash on bad timestamp.
        // Default is FAIL which crashes the stream thread.
        props.put(
                org.apache.kafka.streams.StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class.getName()
        );

        // ── Strategy 4: Production Error Handler ──────────────────────────
        // Called when a record CANNOT be written to the output topic.
        // ALWAYS_CONTINUE: log and skip — don't crash the stream.
        // Default is DefaultProductionExceptionHandler which FAILS.
        props.put(
                org.apache.kafka.streams.StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                AlwaysContinueProductionExceptionHandler.class.getName()
        );

        return props;
    }

    // ── Custom production exception handler ───────────────────────────────
    // Logs the failure and continues — useful for non-critical output topics
    // In production: you might send to a DLT here instead of just logging
    public static class AlwaysContinueProductionExceptionHandler
            implements ProductionExceptionHandler {

        private static final Logger log =
                LoggerFactory.getLogger(AlwaysContinueProductionExceptionHandler.class);

        @Override
        public ProductionExceptionHandlerResponse handle(
                org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> record,
                Exception exception) {
            log.error(" Production error for key={}: {} — continuing",
                    record.key() != null ? new String(record.key()) : "null",
                    exception.getMessage());
            return ProductionExceptionHandlerResponse.CONTINUE;
        }

        @Override
        public void configure(java.util.Map<String, ?> configs) {}
    }
}
