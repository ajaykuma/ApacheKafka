package com.kafka.examples.advanced;

import com.kafka.examples.advanced.errorhandling.StreamsErrorHandling;
import com.kafka.examples.advanced.restapi.InventoryRestApi;
import com.kafka.examples.common.JsonUtil;
import com.kafka.examples.common.JsonUtil.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.*;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@DisplayName("Advanced Topology Tests")
class AdvancedTopologyTests {

    private static Properties testProps() {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-advanced");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Use LogAndContinue for tests so bad records don't crash the test
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                org.apache.kafka.streams.errors.LogAndContinueExceptionHandler.class.getName());
        return props;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Error Handling Tests
    // ══════════════════════════════════════════════════════════════════════
    @Nested
    @DisplayName("Streams Error Handling")
    class ErrorHandlingTests {

        TopologyTestDriver driver;
        TestInputTopic<String, String> input;
        TestOutputTopic<String, String> output;
        TestOutputTopic<String, String> dlt;

        @BeforeEach
        void setUp() {
            driver = new TopologyTestDriver(
                    StreamsErrorHandling.buildTopology(),
                    StreamsErrorHandling.buildStreamsProps());
            input  = driver.createInputTopic(StreamsErrorHandling.TOPIC_INPUT,
                    Serdes.String().serializer(), Serdes.String().serializer());
            output = driver.createOutputTopic(StreamsErrorHandling.TOPIC_OUTPUT,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
            dlt    = driver.createOutputTopic(StreamsErrorHandling.TOPIC_DLT,
                    Serdes.String().deserializer(), Serdes.String().deserializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("Valid order is processed and appears in output topic")
        void validOrderGoesToOutput() {
            var order = new OrderPlacedEvent("ORD-001", "CUST-100", "PROD-A1", 2, 149.99, "PENDING");
            input.pipeInput("ORD-001", JsonUtil.toJson(order));

            assertFalse(output.isEmpty(), "Valid order should appear in output topic");
            assertTrue(dlt.isEmpty(),     "Valid order should NOT go to DLT");

            var result = output.readRecord().getValue();
            assertTrue(result.contains("PROCESSED"), "Output should be marked PROCESSED");
            assertTrue(result.contains("vatAmount"),  "Output should include VAT calculation");
            assertTrue(result.contains("ORD-001"),    "Output should contain order ID");
        }

        @Test
        @DisplayName("VAT is calculated correctly (19%) on valid order")
        void vatCalculatedCorrectly() {
            var order = new OrderPlacedEvent("ORD-002", "CUST-101", "PROD-B3", 1, 100.00, "PENDING");
            input.pipeInput("ORD-002", JsonUtil.toJson(order));

            var result = output.readRecord().getValue();
            assertTrue(result.contains("19.00"), "VAT should be 19.00 for 100.00 order");
            assertTrue(result.contains("119.00"), "Total with VAT should be 119.00");
        }

        @Test
        @DisplayName("Order with negative amount goes to DLT")
        void negativeAmountGoesToDlt() {
            var order = new OrderPlacedEvent("ORD-003", "CUST-100", "PROD-X1", 2, -50.00, "PENDING");
            input.pipeInput("ORD-003", JsonUtil.toJson(order));

            assertTrue(output.isEmpty(), "Invalid order should NOT go to output");
            assertFalse(dlt.isEmpty(),   "Invalid order SHOULD go to DLT");

            var dltRecord = dlt.readRecord().getValue();
            assertTrue(dltRecord.contains("INVALID_AMOUNT"), "DLT record should contain failure reason");
            assertTrue(dltRecord.contains("ORD-003"),        "DLT record should contain original key");
        }

        @Test
        @DisplayName("Malformed JSON goes to DLT")
        void malformedJsonGoesToDlt() {
            input.pipeInput("ORD-004", "NOT_VALID_JSON");

            assertTrue(output.isEmpty(), "Malformed record should NOT go to output");
            assertFalse(dlt.isEmpty(),   "Malformed record SHOULD go to DLT");

            var dltRecord = dlt.readRecord().getValue();
            assertTrue(dltRecord.contains("NOT_JSON") || dltRecord.contains("PARSE_ERROR"),
                    "DLT should contain reason for failure");
        }

        @Test
        @DisplayName("Topology continues processing valid records after bad ones")
        void topologyContinuesAfterBadRecords() {
            // Mix of bad then good
            input.pipeInput("BAD-001", "not json");
            input.pipeInput("BAD-002", JsonUtil.toJson(
                    new OrderPlacedEvent("BAD-002", "CUST-X", "PROD-X", -1, -1.0, "PENDING")));
            input.pipeInput("GOOD-001", JsonUtil.toJson(
                    new OrderPlacedEvent("GOOD-001", "CUST-100", "PROD-A1", 1, 99.99, "PENDING")));
            input.pipeInput("GOOD-002", JsonUtil.toJson(
                    new OrderPlacedEvent("GOOD-002", "CUST-101", "PROD-B3", 2, 49.99, "PENDING")));

            // 2 valid records should be in output
            assertEquals(2, output.readRecordsToList().size(),
                    "Should process exactly 2 valid records despite 2 bad ones before them");

            // 2 bad records should be in DLT
            assertEquals(2, dlt.readRecordsToList().size(),
                    "Should route exactly 2 bad records to DLT");
        }

        @Test
        @DisplayName("DLT record contains original key, reason, and original value")
        void dltRecordIsEnriched() {
            input.pipeInput("ORD-005", "");  // empty string

            var dltRecord = dlt.readRecord().getValue();
            assertTrue(dltRecord.contains("ORD-005"),          "DLT should contain original key");
            assertTrue(dltRecord.contains("reason"),           "DLT should contain failure reason");
            assertTrue(dltRecord.contains("failedAt"),         "DLT should contain failure timestamp");
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Inventory REST API Topology Tests
    // ══════════════════════════════════════════════════════════════════════
    @Nested
    @DisplayName("Inventory REST API Topology")
    class InventoryTopologyTests {

        TopologyTestDriver driver;
        TestInputTopic<String, String> input;

        @BeforeEach
        void setUp() {
            driver = new TopologyTestDriver(InventoryRestApi.buildTopology(), testProps());
            input  = driver.createInputTopic(InventoryRestApi.TOPIC_INVENTORY,
                    Serdes.String().serializer(), Serdes.String().serializer());
        }

        @AfterEach
        void tearDown() { driver.close(); }

        @Test
        @DisplayName("STOCK_IN increases product stock in state store")
        void stockInIncreasesStock() {
            input.pipeInput("PROD-A1", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-A1", "STOCK_IN", 50, 50)));

            var store = driver.getKeyValueStore(InventoryRestApi.STORE_INVENTORY);
            assertNotNull(store.get("PROD-A1"), "Product should be in store after STOCK_IN");

            var inv = JsonUtil.fromJson(
                    store.get("PROD-A1").toString(), InventoryRestApi.ProductInventory.class);
            assertEquals(50, inv.currentStock(), "Stock should be 50 after STOCK_IN of 50");
            assertEquals(1,  inv.eventCount(),   "Event count should be 1");
        }

        @Test
        @DisplayName("STOCK_OUT decreases product stock correctly")
        void stockOutDecreasesStock() {
            input.pipeInput("PROD-B3", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-B3", "STOCK_IN", 100, 100)));
            input.pipeInput("PROD-B3", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-B3", "STOCK_OUT", 30, 70)));

            var store = driver.getKeyValueStore(InventoryRestApi.STORE_INVENTORY);
            var inv = JsonUtil.fromJson(
                    store.get("PROD-B3").toString(), InventoryRestApi.ProductInventory.class);

            assertEquals(70, inv.currentStock(), "Stock should be 70 after 100 in, 30 out");
            assertEquals(2,  inv.eventCount(),   "Should have processed 2 events");
        }

        @Test
        @DisplayName("Stock never goes below zero on STOCK_OUT")
        void stockNeverGoesNegative() {
            input.pipeInput("PROD-C7", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-C7", "STOCK_IN", 10, 10)));
            input.pipeInput("PROD-C7", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-C7", "STOCK_OUT", 999, 0))); // more than available

            var store = driver.getKeyValueStore(InventoryRestApi.STORE_INVENTORY);
            var inv = JsonUtil.fromJson(
                    store.get("PROD-C7").toString(), InventoryRestApi.ProductInventory.class);

            assertEquals(0, inv.currentStock(),
                    "Stock should be 0, not negative, when STOCK_OUT exceeds available");
        }

        @Test
        @DisplayName("ADJUSTMENT sets stock to absolute value")
        void adjustmentSetsAbsoluteStock() {
            input.pipeInput("PROD-D2", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-D2", "STOCK_IN", 50, 50)));
            input.pipeInput("PROD-D2", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-D2", "ADJUSTMENT", 200, 200)));

            var store = driver.getKeyValueStore(InventoryRestApi.STORE_INVENTORY);
            var inv = JsonUtil.fromJson(
                    store.get("PROD-D2").toString(), InventoryRestApi.ProductInventory.class);

            assertEquals(200, inv.currentStock(),
                    "ADJUSTMENT should set stock to absolute value 200, not add to 50");
        }

        @Test
        @DisplayName("Multiple products tracked independently in same store")
        void multipleProductsTrackedIndependently() {
            input.pipeInput("PROD-A1", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-A1", "STOCK_IN", 10, 10)));
            input.pipeInput("PROD-B3", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-B3", "STOCK_IN", 20, 20)));
            input.pipeInput("PROD-A1", JsonUtil.toJson(
                    new WarehouseEvent("WH-01", "PROD-A1", "STOCK_OUT", 3, 7)));

            var store = driver.getKeyValueStore(InventoryRestApi.STORE_INVENTORY);
            var invA1 = JsonUtil.fromJson(
                    store.get("PROD-A1").toString(), InventoryRestApi.ProductInventory.class);
            var invB3 = JsonUtil.fromJson(
                    store.get("PROD-B3").toString(), InventoryRestApi.ProductInventory.class);

            assertEquals(7,  invA1.currentStock(), "PROD-A1 should have 7 (10-3)");
            assertEquals(20, invB3.currentStock(), "PROD-B3 should have 20 (unaffected)");
        }
    }
}
