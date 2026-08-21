package com.kafka.examples.advanced.eos;

import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ADVANCED – Transaction Verifier
 * =================================
 * Proves the core EOS guarantee by running TWO consumers on the same topic:
 *
 *   Consumer A: isolation.level=read_committed  → sees only committed records
 *   Consumer B: isolation.level=read_uncommitted → sees ALL records including aborted
 *
 * Expected result after TransactionalProducer:
 *   read_committed:   3 records (ORD-TXN-001, 002, 004 — ORD-TXN-003 was aborted)
 *   read_uncommitted: 4 records (all including the aborted ORD-TXN-003)
 *
 * This difference IS the EOS guarantee.
 *
 * HOW TO RUN (after TransactionalProducer):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.advanced.eos.TransactionVerifier"
 */
public class TransactionVerifier {

    private static final Logger log = LoggerFactory.getLogger(TransactionVerifier.class);

    public static void main(String[] args) throws Exception {

        log.info("=== Transaction Verification ===");
        log.info("Running two consumers on {} with different isolation levels...\n",
                TransactionalProducer.TOPIC_ORDERS_CONFIRMED);

        // Run both consumers and compare counts
        var committedCount   = countRecords("read_committed",   "verifier-committed-group");
        var uncommittedCount = countRecords("read_uncommitted", "verifier-uncommitted-group");

        log.info("\n=== Results ===");
        log.info("  read_committed   saw: {} records", committedCount);
        log.info("  read_uncommitted saw: {} records", uncommittedCount);
        log.info("  Aborted records hidden from read_committed: {}",
                uncommittedCount - committedCount);

        if (uncommittedCount > committedCount) {
            log.info("\n EOS VERIFIED: {} aborted record(s) invisible to read_committed consumers",
                    uncommittedCount - committedCount);
        } else if (committedCount == uncommittedCount) {
            log.warn("\n Both counts equal — either no aborts occurred or re-running after full commit.");
        }
    }

    private static int countRecords(String isolationLevel, String groupId) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // THE config that makes the difference:
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        var count = new AtomicInteger(0);

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(List.of(TransactionalProducer.TOPIC_ORDERS_CONFIRMED));

            // Poll until no new records for 3 seconds (topic fully consumed)
            long lastRecordTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - lastRecordTime < 3000) {
                var records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    count.incrementAndGet();
                    log.info("  [{}] orderId={} partition={} offset={}",
                            isolationLevel, record.key(), record.partition(), record.offset());
                    lastRecordTime = System.currentTimeMillis();
                }
            }
        }

        log.info("  → [{}] total: {} records\n", isolationLevel, count.get());
        return count.get();
    }
}
