package com.kafka.examples.archetypes.d_sharing;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

/**
 * SECTION 2D – Async Data Sharing Between Systems
 * =================================================
 * Pattern: Kafka as a decoupling layer between domain systems
 * (warehouse, logistics, customer, vehicle) that must share state
 * changes without direct API calls.
 *
 * Topology:
 *   Warehouse System  ──→ [logistics.warehouse.inventory-changed]  ──→ Fulfillment System
 *   Logistics System  ──→ [logistics.shipment.status-changed]      ──→ Notification System
 *                                                                   ──→ Customer Portal
 *
 * Key points:
 *   - Producer doesn't know its consumers (pure decoupling)
 *   - Multiple consumers subscribe independently with different group IDs
 *   - Each consumer processes at its own pace
 *   - No synchronous coupling — warehouse doesn't wait for fulfillment
 *
 * HOW TO RUN:
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.archetypes.d_sharing.AsyncDataSharing"
 */
public class AsyncDataSharing {

    private static final Logger log = LoggerFactory.getLogger(AsyncDataSharing.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                KafkaConfig.TOPIC_WAREHOUSE_EVENTS,
                KafkaConfig.TOPIC_SHIPMENT_STATUS
        );

        // Latch to keep all threads alive until we've seen enough messages
        var latch = new CountDownLatch(1);

        // Use virtual threads — one per consumer group, simulating separate microservices
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {

            // ── Consumers (start first so nothing is missed) ───────────────
            executor.submit(() -> runWarehouseConsumer("fulfillment-service"));
            executor.submit(() -> runWarehouseConsumer("reporting-service"));
            executor.submit(() -> runShipmentConsumer("notification-service"));
            executor.submit(() -> runShipmentConsumer("customer-portal-service"));

            Thread.sleep(1000); // let consumers register

            // ── Producers ─────────────────────────────────────────────────
            executor.submit(AsyncDataSharing::publishWarehouseEvents);
            executor.submit(AsyncDataSharing::publishShipmentEvents);

            // Run for 10 seconds then exit
            Thread.sleep(10_000);
            latch.countDown();
        }
    }

    // ── Warehouse System: publishes inventory changes ──────────────────────
    private static void publishWarehouseEvents() {
        log.info(" Warehouse system: publishing inventory events...");
        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            var random   = new Random();
            var products = List.of("PROD-A1", "PROD-B3", "PROD-C7", "PROD-D2", "PROD-E9");
            var actions  = List.of("STOCK_IN", "STOCK_OUT", "ADJUSTMENT");

            for (int i = 0; i < 12; i++) {
                String productId = products.get(random.nextInt(products.size()));
                String action    = actions.get(random.nextInt(actions.size()));
                int    qty       = random.nextInt(50) + 1;
                int    newStock  = random.nextInt(200);

                var event = new WarehouseEvent("WH-MUNICH-01", productId, action, qty, newStock);

                // KEY = productId → ensures all events for same product go to same partition
                // This guarantees ordering: STOCK_IN before STOCK_OUT for the same product
                producer.send(new ProducerRecord<>(
                        KafkaConfig.TOPIC_WAREHOUSE_EVENTS,
                        productId,
                        JsonUtil.toJson(event)
                ), (meta, ex) -> {
                    if (ex == null)
                        log.info("warehouse event: {} {} qty={} newStock={}",
                                action, productId, qty, newStock);
                });

                Thread.sleep(300);
            }
            producer.flush();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── Logistics System: publishes shipment status changes ───────────────
    private static void publishShipmentEvents() {
        log.info("Logistics system: publishing shipment events...");
        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            // Simulate a shipment progressing through states
            // KEY = shipmentId ensures ordered delivery per shipment
            var shipments = List.of(
                    List.of("SHP-001", "ORD-001", "PICKED",    "Munich Warehouse",     "DHL"),
                    List.of("SHP-001", "ORD-001", "PACKED",    "Munich Warehouse",     "DHL"),
                    List.of("SHP-002", "ORD-002", "PICKED",    "Berlin Warehouse",     "UPS"),
                    List.of("SHP-001", "ORD-001", "SHIPPED",   "Munich Hub",           "DHL"),
                    List.of("SHP-002", "ORD-002", "PACKED",    "Berlin Warehouse",     "UPS"),
                    List.of("SHP-003", "ORD-003", "PICKED",    "Hamburg Warehouse",    "FedEx"),
                    List.of("SHP-001", "ORD-001", "DELIVERED", "Customer Address",     "DHL"),
                    List.of("SHP-002", "ORD-002", "SHIPPED",   "Berlin Hub",           "UPS")
            );

            for (var s : shipments) {
                var event = new ShipmentEvent(s.get(0), s.get(1), s.get(2), s.get(3), s.get(4));
                producer.send(new ProducerRecord<>(
                        KafkaConfig.TOPIC_SHIPMENT_STATUS,
                        s.get(0), // KEY = shipmentId
                        JsonUtil.toJson(event)
                ), (meta, ex) -> {
                    if (ex == null)
                        log.info("shipment event: {} status={} location={}",
                                s.get(0), s.get(2), s.get(3));
                });
                Thread.sleep(400);
            }
            producer.flush();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ── Consumer: warehouse events ─────────────────────────────────────────
    // Each service has its OWN consumer group — they each get ALL messages
    private static void runWarehouseConsumer(String serviceName) {
        try (var consumer = new KafkaConsumer<String, String>(
                KafkaConfig.consumerProps(serviceName + "-warehouse-group"))) {

            consumer.subscribe(List.of(KafkaConfig.TOPIC_WAREHOUSE_EVENTS));
            log.info("[{}] subscribed to warehouse events", serviceName);

            long deadline = System.currentTimeMillis() + 12_000;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    var event = JsonUtil.fromJson(record.value(), WarehouseEvent.class);
                    switch (serviceName) {
                        case "fulfillment-service" ->
                            log.info("[fulfillment]  {} → product={} newStock={} — checking fulfillable orders",
                                    event.action(), event.productId(), event.newStockLevel());
                        case "reporting-service" ->
                            log.info("[reporting]    {} → product={} qty={} — updating inventory report",
                                    event.action(), event.productId(), event.quantity());
                    }
                    consumer.commitSync();
                }
            }
        }
    }

    // ── Consumer: shipment status events ──────────────────────────────────
    private static void runShipmentConsumer(String serviceName) {
        try (var consumer = new KafkaConsumer<String, String>(
                KafkaConfig.consumerProps(serviceName + "-shipment-group"))) {

            consumer.subscribe(List.of(KafkaConfig.TOPIC_SHIPMENT_STATUS));
            log.info("[{}] subscribed to shipment events", serviceName);

            long deadline = System.currentTimeMillis() + 12_000;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                for (var record : records) {
                    var event = JsonUtil.fromJson(record.value(), ShipmentEvent.class);
                    switch (serviceName) {
                        case "notification-service" -> {
                            if (List.of("SHIPPED", "DELIVERED").contains(event.status())) {
                                log.info("[notifications] Sending '{}' email for shipment={} order={}",
                                        event.status(), event.shipmentId(), event.orderId());
                            }
                        }
                        case "customer-portal-service" ->
                            log.info("[customer-portal] Updating tracking page: {} → {} @ {}",
                                    event.shipmentId(), event.status(), event.location());
                    }
                    consumer.commitSync();
                }
            }
        }
    }
}
