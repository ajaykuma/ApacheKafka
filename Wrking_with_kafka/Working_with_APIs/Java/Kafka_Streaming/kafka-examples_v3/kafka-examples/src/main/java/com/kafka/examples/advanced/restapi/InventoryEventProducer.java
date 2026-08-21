package com.kafka.examples.advanced.restapi;

import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Produces inventory events to populate the REST API state store.
 * After running this, query the REST API to see live inventory state.
 *
 * HOW TO RUN (while InventoryRestApi is running):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.restapi.InventoryEventProducer"
 *
 * Then query:
 *   curl http://localhost:7070/api/products
 *   curl http://localhost:7070/api/products/PROD-A1
 *   curl http://localhost:7070/api/products/low-stock
 *   curl http://localhost:7070/api/stats
 *
 * On Windows without curl, open these URLs directly in your browser.
 */
public class InventoryEventProducer {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventProducer.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(InventoryRestApi.TOPIC_INVENTORY);

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            // Initial stock levels
            log.info("Phase 1: Initial stock intake...");
            sendEvents(producer, List.of(
                new WarehouseEvent("WH-01", "PROD-A1", "STOCK_IN",  50, 50),
                new WarehouseEvent("WH-01", "PROD-B3", "STOCK_IN",  30, 30),
                new WarehouseEvent("WH-01", "PROD-C7", "STOCK_IN", 100, 100),
                new WarehouseEvent("WH-01", "PROD-D2", "STOCK_IN",  25, 25),
                new WarehouseEvent("WH-01", "PROD-E9", "STOCK_IN",  15, 15)  // starts low stock
            ));
            Thread.sleep(1000);
            log.info("→ Query now: http://localhost:{}/api/products", InventoryRestApi.REST_PORT);

            // Sales activity
            log.info("Phase 2: Sales activity...");
            sendEvents(producer, List.of(
                new WarehouseEvent("WH-01", "PROD-A1", "STOCK_OUT", 35, 15), // drops low
                new WarehouseEvent("WH-01", "PROD-B3", "STOCK_OUT", 12, 18),
                new WarehouseEvent("WH-01", "PROD-C7", "STOCK_OUT",  5, 95),
                new WarehouseEvent("WH-01", "PROD-D2", "STOCK_OUT", 20,  5)  // drops low
            ));
            Thread.sleep(1000);
            log.info("→ Query now: http://localhost:{}/api/products/low-stock", InventoryRestApi.REST_PORT);

            // Restock some products
            log.info("Phase 3: Restocking...");
            sendEvents(producer, List.of(
                new WarehouseEvent("WH-01", "PROD-A1", "STOCK_IN", 100, 115), // restocked
                new WarehouseEvent("WH-01", "PROD-E9", "STOCK_IN",  50,  65), // restocked
                new WarehouseEvent("WH-01", "PROD-C7", "ADJUSTMENT", 200, 200) // manual adjustment
            ));
            Thread.sleep(1000);
            log.info("→ Query now: http://localhost:{}/api/stats", InventoryRestApi.REST_PORT);

            producer.flush();
            log.info("\n All inventory events published.");
            log.info("   REST API is still running — keep querying:");
            log.info("   http://localhost:{}/api/products", InventoryRestApi.REST_PORT);
            log.info("   http://localhost:{}/api/products/PROD-A1", InventoryRestApi.REST_PORT);
            log.info("   http://localhost:{}/api/products/low-stock", InventoryRestApi.REST_PORT);
            log.info("   http://localhost:{}/api/stats", InventoryRestApi.REST_PORT);
        }
    }

    private static void sendEvents(
            KafkaProducer<String, String> producer, List<WarehouseEvent> events) {
        events.forEach(event -> {
            producer.send(new ProducerRecord<>(
                    InventoryRestApi.TOPIC_INVENTORY,
                    event.productId(),
                    JsonUtil.toJson(event)
            ), (m, e) -> {
                if (e == null)
                    log.info(" {} {} qty={}", event.action(), event.productId(), event.quantity());
            });
        });
        producer.flush();
    }
}
