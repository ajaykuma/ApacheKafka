package com.kafkaops.performance;

/**
 * Rough partition-count advisor based on the standard Kafka sizing formula:
 *
 *   partitions = max(targetThroughputMBps / producerThroughputPerPartitionMBps,
 *                     targetThroughputMBps / consumerThroughputPerPartitionMBps)
 *
 * This is intentionally a back-of-envelope tool, not a guarantee — real
 * sizing also depends on key distribution (hot keys), message size,
 * replication factor, and broker disk/network headroom. Treat the output
 * as a starting point to load-test against, not a final answer.
 *
 * Rules of thumb baked in below:
 *  - More partitions = more parallelism (good) but also more open file
 *    handles, longer leader elections, and slower full-cluster rebalances
 *    (bad). Most production clusters stay well under a few thousand
 *    partitions per broker.
 *  - You can't have more *active* consumers in a group than partitions —
 *    extra consumers just sit idle. So partition count is also your
 *    ceiling on consumer-side parallelism.
 *
 * Usage:
 *   mvn exec:java -Dexec.mainClass=com.kafkaops.performance.PartitionSizingAdvisor \
 *       -Dexec.args="100 10 20"
 *   (target throughput MB/s, producer MB/s per partition, consumer MB/s per partition)
 */
public class PartitionSizingAdvisor {

    public static void main(String[] args) {
        double targetThroughputMBps = args.length > 0 ? Double.parseDouble(args[0]) : 100;
        double producerThroughputPerPartition = args.length > 1 ? Double.parseDouble(args[1]) : 10;
        double consumerThroughputPerPartition = args.length > 2 ? Double.parseDouble(args[2]) : 20;
        int plannedConsumerInstances = args.length > 3 ? Integer.parseInt(args[3]) : 1;

        double byProducer = targetThroughputMBps / producerThroughputPerPartition;
        double byConsumer = targetThroughputMBps / consumerThroughputPerPartition;
        int recommended = (int) Math.ceil(Math.max(byProducer, byConsumer));

        // Round up to at least the number of consumer instances so none sit idle.
        recommended = Math.max(recommended, plannedConsumerInstances);

        System.out.println("=== Partition Sizing Advisor ===");
        System.out.printf("Target throughput        : %.1f MB/s%n", targetThroughputMBps);
        System.out.printf("Producer MB/s/partition   : %.1f%n", producerThroughputPerPartition);
        System.out.printf("Consumer MB/s/partition   : %.1f%n", consumerThroughputPerPartition);
        System.out.println();
        System.out.printf("Partitions needed (producer-bound) : %.1f -> %d%n", byProducer, (int) Math.ceil(byProducer));
        System.out.printf("Partitions needed (consumer-bound) : %.1f -> %d%n", byConsumer, (int) Math.ceil(byConsumer));
        System.out.println();
        System.out.println("Recommended partition count: " + recommended);
        System.out.println();
        System.out.println("Notes:");
        System.out.println(" - This is a starting point; validate with a real load test on your topic.");
        System.out.println(" - Leave headroom for growth — repartitioning later requires either over-provisioning");
        System.out.println("   up front or a migration (new topic + dual-write/backfill), since you can't safely");
        System.out.println("   reduce partitions and increasing them changes key-to-partition mapping.");
        System.out.println(" - Very large partition counts (1000s per broker) slow controller failover and");
        System.out.println("   metadata propagation — there's a real ceiling, not just an availability one.");
    }
}
