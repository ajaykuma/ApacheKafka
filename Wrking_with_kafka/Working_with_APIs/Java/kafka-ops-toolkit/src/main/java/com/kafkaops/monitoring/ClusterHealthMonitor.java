package com.kafkaops.monitoring;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Cluster health snapshot: broker count, controller, and per-partition
 * replica / ISR state for every topic (or a filtered subset).
 *
 * Why this matters operationally:
 *  - "ISR" (In-Sync Replicas) shrinking below the full replica set is one of
 *    the earliest signs of broker stress, network partition, or a slow disk.
 *  - Losing the controller or having brokers flap is visible here before
 *    it shows up as consumer-facing errors.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.monitoring.ClusterHealthMonitor \
 *       -Dexec.args="localhost:9092"
 */
public class ClusterHealthMonitor {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
        String topicFilter = args.length > 1 ? args[1] : null; // optional: only show this topic

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", "10000");

        try (AdminClient admin = AdminClient.create(props)) {
            printClusterOverview(admin);
            printTopicHealth(admin, topicFilter);
        }
    }

    private static void printClusterOverview(AdminClient admin) throws ExecutionException, InterruptedException {
        DescribeClusterResult cluster = admin.describeCluster();
        Collection<Node> nodes = cluster.nodes().get();
        Node controller = cluster.controller().get();
        String clusterId = cluster.clusterId().get();

        System.out.println("=== Cluster Overview ===");
        System.out.println("Cluster ID : " + clusterId);
        System.out.println("Brokers    : " + nodes.size());
        for (Node n : nodes) {
            String marker = n.id() == controller.id() ? "  (controller)" : "";
            System.out.printf("  - id=%d host=%s:%d%s%n", n.id(), n.host(), n.port(), marker);
        }
        System.out.println();
    }

    private static void printTopicHealth(AdminClient admin, String topicFilter)
            throws ExecutionException, InterruptedException {
        Set<String> topicNames = admin.listTopics().names().get();
        if (topicFilter != null) {
            topicNames = topicNames.stream()
                    .filter(t -> t.equals(topicFilter))
                    .collect(java.util.stream.Collectors.toSet());
        }
        if (topicNames.isEmpty()) {
            System.out.println("No matching topics found.");
            return;
        }

        Map<String, TopicDescription> descriptions =
                admin.describeTopics(topicNames).allTopicNames().get();

        System.out.println("=== Topic / Partition Health (ISR check) ===");
        int unhealthyPartitions = 0;

        for (TopicDescription desc : descriptions.values()) {
            System.out.println("Topic: " + desc.name() + " (internal=" + desc.isInternal() + ")");
            for (TopicPartitionInfo p : desc.partitions()) {
                int replicaCount = p.replicas().size();
                int isrCount = p.isr().size();
                boolean healthy = replicaCount == isrCount;
                if (!healthy) unhealthyPartitions++;

                System.out.printf("  partition=%d leader=%s replicas=%d isr=%d %s%n",
                        p.partition(),
                        p.leader() == null ? "NONE(!!)" : p.leader().idString(),
                        replicaCount,
                        isrCount,
                        healthy ? "OK" : "<<< UNDER-REPLICATED");
            }
        }

        System.out.println();
        if (unhealthyPartitions == 0) {
            System.out.println("All partitions fully in-sync.");
        } else {
            System.out.println("WARNING: " + unhealthyPartitions +
                    " partition(s) under-replicated. Investigate broker health / network.");
        }
    }
}
