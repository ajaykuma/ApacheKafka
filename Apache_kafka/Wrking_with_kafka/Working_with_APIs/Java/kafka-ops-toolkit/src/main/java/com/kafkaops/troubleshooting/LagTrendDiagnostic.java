package com.kafkaops.troubleshooting;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Samples total consumer-group lag a few times over a short window and
 * classifies the trend, instead of just giving you a single snapshot number.
 * A single lag reading can't tell you if you have a problem; the *trend*
 * can:
 *
 *   RECOVERING : lag dropping steadily        -> probably fine, was a blip
 *   STABLE     : lag flat and low/zero         -> healthy steady state
 *   STUCK      : lag flat but non-zero         -> consumer likely stalled
 *                (check for a poison-pill message or a stuck downstream call)
 *   GROWING    : lag increasing                -> consumer can't keep up;
 *                see Performance Tuning module, or scale out consumers
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.LagTrendDiagnostic \
 *       -Dexec.args="localhost:9092 my-group 5 3000"
 *   (bootstrap, group, sampleCount, intervalMs)
 */
public class LagTrendDiagnostic {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Usage: LagTrendDiagnostic <bootstrap-servers> <group-id> [sampleCount] [intervalMs]");
            return;
        }
        String bootstrapServers = args[0];
        String groupId = args[1];
        int sampleCount = args.length > 2 ? Integer.parseInt(args[2]) : 5;
        long intervalMs = args.length > 3 ? Long.parseLong(args[3]) : 3000;

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        List<Long> samples = new ArrayList<>();

        try (AdminClient admin = AdminClient.create(props)) {
            for (int i = 0; i < sampleCount; i++) {
                long lag = totalLag(admin, groupId);
                samples.add(lag);
                System.out.println("Sample " + (i + 1) + "/" + sampleCount + ": total lag = " + lag);
                if (i < sampleCount - 1) Thread.sleep(intervalMs);
            }
        }

        System.out.println();
        System.out.println("=== Diagnosis ===");
        System.out.println(classify(samples));
    }

    private static long totalLag(AdminClient admin, String groupId) throws ExecutionException, InterruptedException {
        Map<TopicPartition, OffsetAndMetadata> committed =
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        if (committed.isEmpty()) return 0;

        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition tp : committed.keySet()) request.put(tp, OffsetSpec.latest());
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                admin.listOffsets(request).all().get();

        long total = 0;
        for (var entry : committed.entrySet()) {
            long end = endOffsets.get(entry.getKey()).offset();
            total += Math.max(0, end - entry.getValue().offset());
        }
        return total;
    }

    private static String classify(List<Long> samples) {
        if (samples.size() < 2) return "Not enough samples to classify a trend.";

        long first = samples.get(0);
        long last = samples.get(samples.size() - 1);
        long max = Collections.max(samples);
        long min = Collections.min(samples);
        boolean roughlyFlat = (max - min) <= Math.max(5, first * 0.05); // within 5% or 5 records

        if (roughlyFlat && last <= 5) {
            return "STABLE: lag is consistently near zero. Healthy.";
        }
        if (roughlyFlat && last > 5) {
            return "STUCK: lag is non-zero but not moving. Likely a stalled consumer " +
                    "(poison-pill message, deadlocked downstream call, or a paused partition). " +
                    "Check consumer logs and thread state, not just throughput config.";
        }
        if (last < first) {
            return "RECOVERING: lag is trending down (" + first + " -> " + last + "). " +
                    "Probably a transient blip (deploy, GC pause, brief traffic spike). Keep watching.";
        }
        return "GROWING: lag is trending up (" + first + " -> " + last + "). " +
                "Consumer can't keep pace with producer rate. Consider: more consumer instances " +
                "(up to partition count), faster per-record processing, or larger max.poll.records " +
                "with async downstream calls.";
    }
}
