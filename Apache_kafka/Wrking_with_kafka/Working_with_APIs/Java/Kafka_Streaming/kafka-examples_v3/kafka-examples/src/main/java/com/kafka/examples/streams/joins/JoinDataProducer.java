package com.kafka.examples.streams.joins;

import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Produces test data for all three join types in JoinExamples.
 *
 * HOW TO RUN (while JoinExamples is running):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.joins.JoinDataProducer"
 */
public class JoinDataProducer {

    private static final Logger log = LoggerFactory.getLogger(JoinDataProducer.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(
                JoinExamples.TOPIC_CLICKS, JoinExamples.TOPIC_PURCHASES,
                JoinExamples.TOPIC_USER_PREFS, JoinExamples.TOPIC_REGION_MAP
        );

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            // ── Seed GlobalKTable: region lookup ───────────────────────────
            log.info("Seeding region map (GlobalKTable)...");
            producer.send(new ProducerRecord<>(JoinExamples.TOPIC_REGION_MAP, "EU",
                    "{\"code\":\"EU\",\"name\":\"Europe\",\"currency\":\"EUR\",\"tz\":\"CET\"}"));
            producer.send(new ProducerRecord<>(JoinExamples.TOPIC_REGION_MAP, "US",
                    "{\"code\":\"US\",\"name\":\"United States\",\"currency\":\"USD\",\"tz\":\"EST\"}"));
            producer.flush();
            Thread.sleep(1000);

            // ── Seed KTable: user preferences ──────────────────────────────
            log.info("Seeding user preferences (KTable)...");
            List.of(
                new String[]{"U-001", "{\"userId\":\"U-001\",\"theme\":\"dark\",\"lang\":\"de\",\"region\":\"EU\"}"},
                new String[]{"U-002", "{\"userId\":\"U-002\",\"theme\":\"light\",\"lang\":\"en\",\"region\":\"US\"}"},
                new String[]{"U-003", "{\"userId\":\"U-003\",\"theme\":\"dark\",\"lang\":\"fr\",\"region\":\"EU\"}"}
            ).forEach(p -> producer.send(new ProducerRecord<>(JoinExamples.TOPIC_USER_PREFS, p[0], p[1])));
            producer.flush();
            Thread.sleep(2000); // let streams load KTable and GlobalKTable

            // ── Publish click events (KStream-KStream + KStream-KTable joins) ──
            log.info("Publishing click events...");
            List.of(
                new String[]{"U-001", "{\"userId\":\"U-001\",\"page\":\"/product/A1\",\"region\":\"EU\",\"ts\":" + System.currentTimeMillis() + "}"},
                new String[]{"U-002", "{\"userId\":\"U-002\",\"page\":\"/product/B3\",\"region\":\"US\",\"ts\":" + System.currentTimeMillis() + "}"},
                new String[]{"U-003", "{\"userId\":\"U-003\",\"page\":\"/product/C7\",\"region\":\"EU\",\"ts\":" + System.currentTimeMillis() + "}"},
                new String[]{"U-004", "{\"userId\":\"U-004\",\"page\":\"/product/A1\",\"region\":\"US\",\"ts\":" + System.currentTimeMillis() + "}"}
            ).forEach(p -> {
                producer.send(new ProducerRecord<>(JoinExamples.TOPIC_CLICKS, p[0], p[1]));
                log.info(" Click: userId={}", p[0]);
            });
            producer.flush();
            Thread.sleep(500);

            // ── Publish purchases close in time for KStream-KStream join ───
            log.info("Publishing purchase events (within join window)...");
            List.of(
                new String[]{"U-001", "{\"userId\":\"U-001\",\"productId\":\"A1\",\"amount\":149.99,\"ts\":" + System.currentTimeMillis() + "}"},
                new String[]{"U-003", "{\"userId\":\"U-003\",\"productId\":\"C7\",\"amount\":49.95, \"ts\":" + System.currentTimeMillis() + "}"}
            ).forEach(p -> {
                producer.send(new ProducerRecord<>(JoinExamples.TOPIC_PURCHASES, p[0], p[1]));
                log.info(" Purchase: userId={}", p[0]);
            });
            producer.flush();
            log.info(" All join test data published.");
        }
    }
}
