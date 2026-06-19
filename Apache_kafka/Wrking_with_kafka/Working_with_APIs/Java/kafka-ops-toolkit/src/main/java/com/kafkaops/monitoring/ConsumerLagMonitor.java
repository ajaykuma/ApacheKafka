package com.kafkaops.monitoring;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Reports consumer lag (committed offset vs. log-end offset) for one
 * consumer group, broken down by partition, plus the group's overall state.
 *
 * Lag is the single most useful early-warning metric in Kafka ops:
 *  - Rising lag with steady throughput  -> consumer too slow / needs scaling
 *  - Lag spikes that then recover        -> normal (deploys, GC pauses)
 *  - Lag that never recovers             -> consumer stuck or crash-looping
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.monitoring.ConsumerLagMonitor \
 *       -Dexec.args="localhost:9092 my-consumer-group"
 */
public class ConsumerLagMonitor {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Usage: ConsumerLagMonitor <bootstrap-servers> <group-id>");
            return;
        }
        String bootstrapServers = args[0];
        String groupId = args[1];

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);

        try (AdminClient admin = AdminClient.create(props)) {
            ConsumerGroupDescription groupDesc =
                    admin.describeConsumerGroups(List.of(groupId)).describedGroups().get(groupId).get();

            System.out.println("=== Consumer Group: " + groupId + " ===");
            System.out.println("State   : " + groupDesc.state());
            System.out.println("Members : " + groupDesc.members().size());
            System.out.println();

            Map<TopicPartition, OffsetAndMetadata> committed =
                    admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();

            if (committed.isEmpty()) {
                System.out.println("No committed offsets found for this group (idle or new group).");
                return;
            }

            Map<TopicPartition, OffsetSpec> endOffsetRequest = new HashMap<>();
            for (TopicPartition tp : committed.keySet()) {
                endOffsetRequest.put(tp, OffsetSpec.latest());
            }
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> endOffsets =
                    admin.listOffsets(endOffsetRequest).all().get();

            long totalLag = 0;
            System.out.printf("%-30s %-5s %-15s %-15s %-10s%n",
                    "TOPIC", "PART", "COMMITTED", "LOG-END", "LAG");

            // Sort for stable, readable output
            List<TopicPartition> sorted = new ArrayList<>(committed.keySet());
            sorted.sort(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition));

            for (TopicPartition tp : sorted) {
                long committedOffset = committed.get(tp).offset();
                long endOffset = endOffsets.get(tp).offset();
                long lag = Math.max(0, endOffset - committedOffset);
                totalLag += lag;

                System.out.printf("%-30s %-5d %-15d %-15d %-10d%n",
                        tp.topic(), tp.partition(), committedOffset, endOffset, lag);
            }

            System.out.println();
            System.out.println("TOTAL LAG across all partitions: " + totalLag);
            if (totalLag > 10_000) {
                System.out.println("WARNING: lag is high — check consumer throughput, " +
                        "partition count vs. consumer instance count, and processing time per record.");
            }
        }
    }
}
