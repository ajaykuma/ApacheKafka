package com.kafka.examples.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Central configuration for all Kafka examples.
 *
 * Change BOOTSTRAP_SERVERS if your Kafka is on a different host/port.
 */
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    // ── Connection ─────────────────────────────────────────────────────────
    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    // ── Topic names used across examples ──────────────────────────────────
    // Section 2A – Event-Driven Microservices
    public static final String TOPIC_ACCOUNT_DELETED        = "customer.account.deleted";
    public static final String TOPIC_VEHICLE_MAPPED         = "customer.vehicle.mapped";
    public static final String TOPIC_ORDER_PLACED           = "logistics.order.placed";
    public static final String TOPIC_NOTIFICATIONS          = "notification.send-requested";

    // Section 2B – Data Streaming & Enrichment
    public static final String TOPIC_RAW_ORDERS             = "enrichment.orders.raw";
    public static final String TOPIC_CUSTOMER_PROFILES      = "enrichment.customer-profiles";
    public static final String TOPIC_ENRICHED_ORDERS        = "enrichment.orders.enriched";

    // Section 2C – Ingestion & Distribution
    public static final String TOPIC_SERVICENOW_INBOUND     = "integration.servicenow.inbound";
    public static final String TOPIC_SAP_EVENTS             = "integration.sap.material-events";
    public static final String TOPIC_INGESTION_DLT          = "integration.servicenow.inbound.DLT";

    // Section 2D – Async Data Sharing
    public static final String TOPIC_WAREHOUSE_EVENTS       = "logistics.warehouse.inventory-changed";
    public static final String TOPIC_SHIPMENT_STATUS        = "logistics.shipment.status-changed";

    // Section 3 – Kafka Streams
    public static final String TOPIC_STREAMS_INPUT          = "streams.words.input";
    public static final String TOPIC_STREAMS_OUTPUT         = "streams.words.output";
    public static final String TOPIC_PAGE_VIEWS             = "streams.page-views";
    public static final String TOPIC_USER_PROFILES          = "streams.user-profiles";
    public static final String TOPIC_ENRICHED_PAGE_VIEWS    = "streams.page-views.enriched";
    public static final String TOPIC_SENSOR_READINGS        = "streams.sensor.readings";
    public static final String TOPIC_SENSOR_ALERTS          = "streams.sensor.alerts";
    public static final String TOPIC_CLICK_EVENTS           = "streams.click-events";
    public static final String TOPIC_CLICK_COUNTS_WINDOW    = "streams.click-counts.windowed";
    public static final String TOPIC_STOCK_PRICES           = "streams.stock.prices";
    public static final String TOPIC_STOCK_ALERTS           = "streams.stock.alerts";

    // ── Producer config ────────────────────────────────────────────────────
    public static Properties producerProps() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Best practice: idempotent producer
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        return props;
    }

    // ── Consumer config ────────────────────────────────────────────────────
    public static Properties consumerProps(String groupId) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Best practice: manual commit for reliable processing
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        return props;
    }

    // ── Kafka Streams config ───────────────────────────────────────────────
    public static Properties streamsProps(String applicationId) {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        // Best practice: 1 standby replica for faster rebalancing
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 0); // set to 1 if you have 2+ brokers
        // Best practice: persistent state dir (not /tmp)
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-examples");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1); // 1 for local dev
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        return props;
    }

    // ── Topic creation helper ──────────────────────────────────────────────
    public static void createTopicsIfAbsent(String... topicNames) {
        var adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        try (var admin = AdminClient.create(adminProps)) {
            var existing = admin.listTopics().names().get();
            var toCreate = Arrays.stream(topicNames)
                    .filter(t -> !existing.contains(t))
                    .map(t -> new NewTopic(t, 3, (short) 1)) // 3 partitions, RF=1 for local dev
                    .toList();

            if (!toCreate.isEmpty()) {
                admin.createTopics(toCreate).all().get();
                toCreate.forEach(t -> log.info(" Created topic: {}", t.name()));
            } else {
                log.info("Topics already exist, skipping creation");
            }
        } catch (InterruptedException | ExecutionException e) {
            log.warn("Topic creation warning (may already exist): {}", e.getMessage());
        }
    }
}
