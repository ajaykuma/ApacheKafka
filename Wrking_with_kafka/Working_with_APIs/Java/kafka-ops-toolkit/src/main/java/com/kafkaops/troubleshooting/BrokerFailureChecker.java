package com.kafkaops.troubleshooting;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Detects symptoms of broker failure from the client side:
 *  - Partitions with no leader (leader election failed / all replicas down)
 *  - Partitions whose ISR has shrunk to fewer replicas than configured
 *  - Brokers that exist in topic metadata's replica list but aren't in the
 *    live cluster node list (i.e., a broker that's down)
 *
 * This won't tell you *why* a broker is down (disk full, OOM, network
 * partition, process crash) — for that you need broker-side logs and host
 * metrics — but it tells you precisely which topics/partitions are
 * currently impacted, which is usually the first question during an
 * incident.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.troubleshooting.BrokerFailureChecker \
 *       -Dexec.args="localhost:9092"
 */
public class BrokerFailureChecker {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("request.timeout.ms", "10000");

        try (AdminClient admin = AdminClient.create(props)) {
            Set<Integer> liveBrokerIds = new HashSet<>();
            for (Node n : admin.describeCluster().nodes().get()) liveBrokerIds.add(n.id());

            System.out.println("Live brokers: " + liveBrokerIds);
            System.out.println();

            Set<String> topics = admin.listTopics().names().get();
            Map<String, TopicDescription> descriptions = admin.describeTopics(topics).allTopicNames().get();

            boolean foundIssue = false;

            for (TopicDescription desc : descriptions.values()) {
                for (TopicPartitionInfo p : desc.partitions()) {

                    // 1. No leader at all - the worst case, partition is unavailable for writes
                    if (p.leader() == null) {
                        foundIssue = true;
                        System.out.printf("CRITICAL: %s-%d has NO LEADER. Partition is unavailable.%n",
                                desc.name(), p.partition());
                        continue;
                    }

                    // 2. Replicas referencing broker IDs that aren't currently live
                    List<Integer> downReplicas = p.replicas().stream()
                            .map(Node::id)
                            .filter(id -> !liveBrokerIds.contains(id))
                            .toList();
                    if (!downReplicas.isEmpty()) {
                        foundIssue = true;
                        System.out.printf("WARNING: %s-%d has replica(s) on down broker(s) %s.%n",
                                desc.name(), p.partition(), downReplicas);
                    }

                    // 3. ISR shrunk below full replica set
                    if (p.isr().size() < p.replicas().size()) {
                        foundIssue = true;
                        System.out.printf("WARNING: %s-%d under-replicated: isr=%d/%d replicas.%n",
                                desc.name(), p.partition(), p.isr().size(), p.replicas().size());
                    }
                }
            }

            System.out.println();
            if (!foundIssue) {
                System.out.println("No broker-failure symptoms detected: all partitions have a leader " +
                        "and full ISR, all replicas reference live brokers.");
            } else {
                System.out.println("Next steps: check broker logs (controller.log, server.log) on the " +
                        "affected broker IDs for OOM, disk-full, or GC-pause errors; check host-level " +
                        "metrics (disk, network, CPU) for the same time window.");
            }
        }
    }
}
