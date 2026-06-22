package com.kafka.examples.streams.windowing;

import com.kafka.examples.config.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * Produces click events designed to exercise all three window types:
 *   - Burst of clicks within 10s  → fills a tumbling window
 *   - Clicks spread across 30s    → spans multiple hopping windows
 *   - Gap of 16s+ between clicks  → closes a session window
 *
 * HOW TO RUN (while WindowingExamples is running):
 *   mvn exec:java -Dexec.mainClass="com.kafka.examples.streams.windowing.ClickEventProducer"
 */
public class ClickEventProducer {

    private static final Logger log = LoggerFactory.getLogger(ClickEventProducer.class);

    public static void main(String[] args) throws Exception {

        KafkaConfig.createTopicsIfAbsent(WindowingExamples.TOPIC_CLICKS);

        try (var producer = new KafkaProducer<String, String>(KafkaConfig.producerProps())) {

            var pages = List.of("/home", "/product/A1", "/product/B3", "/cart", "/checkout", "/profile");
            var users = List.of("U-001", "U-002", "U-003");
            var rand  = new Random();

            log.info("Phase 1: Burst of clicks (same 10s tumbling window)...");
            for (int i = 0; i < 8; i++) {
                String user = users.get(rand.nextInt(users.size()));
                String page = pages.get(rand.nextInt(pages.size()));
                producer.send(new ProducerRecord<>(
                        WindowingExamples.TOPIC_CLICKS, user,
                        String.format("{\"userId\":\"%s\",\"page\":\"%s\",\"ts\":%d}", user, page, System.currentTimeMillis())
                ), (m, e) -> { if (e == null) log.info(" click: user={} page={}", user, page); });
                Thread.sleep(800); // 8 clicks × 0.8s = ~6.4s total, fits in one 10s tumbling window
            }
            producer.flush();

            log.info("Phase 2: Pause 12s (crosses tumbling window boundary)...");
            Thread.sleep(12_000);

            log.info("Phase 3: More clicks in next tumbling window...");
            for (int i = 0; i < 5; i++) {
                String user = users.get(rand.nextInt(users.size()));
                String page = pages.get(rand.nextInt(pages.size()));
                producer.send(new ProducerRecord<>(
                        WindowingExamples.TOPIC_CLICKS, user,
                        String.format("{\"userId\":\"%s\",\"page\":\"%s\",\"ts\":%d}", user, page, System.currentTimeMillis())
                ), (m, e) -> { if (e == null) log.info(" click: user={} page={}", user, page); });
                Thread.sleep(1000);
            }
            producer.flush();

            log.info("Phase 4: Pause 20s to close all session windows (gap > 15s)...");
            Thread.sleep(20_000);

            log.info("Phase 5: New clicks → new session windows...");
            for (int i = 0; i < 4; i++) {
                String user = users.get(rand.nextInt(users.size()));
                producer.send(new ProducerRecord<>(
                        WindowingExamples.TOPIC_CLICKS, user,
                        String.format("{\"userId\":\"%s\",\"page\":\"/new-session\",\"ts\":%d}", user, System.currentTimeMillis())
                ), (m, e) -> { if (e == null) log.info(" new-session click: user={}", user); });
                Thread.sleep(500);
            }
            producer.flush();

            log.info(" All click events published. Watch WindowingExamples for window output.");
        }
    }
}
