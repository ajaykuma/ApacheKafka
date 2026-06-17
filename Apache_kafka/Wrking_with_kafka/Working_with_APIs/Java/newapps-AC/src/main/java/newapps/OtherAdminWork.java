package newapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterResult;
//import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
//import org.apache.kafka.clients.admin.NewPartitions;
//import org.apache.kafka.clients.admin.OffsetSpec;
//import org.apache.kafka.clients.admin.RecordsToDelete;
//import org.apache.kafka.clients.admin.DescribeTopicsOptions;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
//import org.apache.kafka.clients.admin.TopicListing;
//import org.apache.kafka.clients.consumer.OffsetAndMetadata;
//import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.errors.ElectionNotNeededException;
//import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.NoReassignmentInProgressException;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
//import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
//import java.util.Collection;
import java.util.Properties;
//import java.util.Set;
import java.util.concurrent.ExecutionException;

public class OtherAdminWork {
	public static final String TOPIC_NAME = "Topic-new";
	public static final String CONSUMER_GROUP = "ApplGroup";
	public static final List<String> TOPIC_LIST = Collections.singletonList(TOPIC_NAME);
	public static final List<String> CONSUMER_GRP_LIST = Collections.singletonList(CONSUMER_GROUP);
	public static final int NUM_PARTITIONS = 3;
	public static final short REP_FACTOR = 3;

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Properties config = new Properties();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "c2:9092");
		AdminClient admin = AdminClient.create(config);

		// Who are the brokers? Who is the controller?
		DescribeClusterResult cluster = admin.describeCluster();

		System.out.println("Connected to cluster " + cluster.clusterId().get());
		System.out.println("The brokers in the cluster are:");
		cluster.nodes().get().forEach(node -> System.out.println("    * " + node));
		System.out.println("The controller is: " + cluster.controller().get());

		// add partitions
		/*
		 * Map<String, NewPartitions> newPartitions = new HashMap<>();
		 * newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(NUM_PARTITIONS+2));
		 * try { admin.createPartitions(newPartitions).all().get(); } catch
		 * (ExecutionException e) { if (e.getCause() instanceof
		 * InvalidPartitionsException) {
		 * System.out.printf("Couldn't modify number of partitions in topic: " +
		 * e.getMessage()); } }
		 */

		// delete records
		// Get offsets committed by the group
		/*
		 * Map<TopicPartition, OffsetAndMetadata> offsets =
		 * admin.listConsumerGroupOffsets(CONSUMER_GROUP)
		 * .partitionsToOffsetAndMetadata().get(); Map<TopicPartition, OffsetSpec>
		 * requestLatestOffsets = new HashMap<>(); Map<TopicPartition, OffsetSpec>
		 * requestEarliestOffsets = new HashMap<>(); Map<TopicPartition, OffsetSpec>
		 * requestOlderOffsets = new HashMap<>(); Map<TopicPartition,
		 * ListOffsetsResult.ListOffsetsResultInfo> olderOffsets =
		 * admin.listOffsets(requestOlderOffsets).all().get(); Map<TopicPartition,
		 * RecordsToDelete> recordsToDelete = new HashMap<>(); for
		 * (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e:
		 * olderOffsets.entrySet()) recordsToDelete.put(e.getKey(),
		 * RecordsToDelete.beforeOffset(e.getValue().offset()));
		 * admin.deleteRecords(recordsToDelete).all().get();
		 */
		/*
		 * Set<TopicPartition> electableTopics = new HashSet<>();
		 * electableTopics.add(new TopicPartition(TOPIC_NAME, 0)); try {
		 * admin.electLeaders(ElectionType.PREFERRED, electableTopics).all().get(); }
		 * catch (ExecutionException e) { if (e.getCause() instanceof
		 * ElectionNotNeededException) { System.out.
		 * println("All leaders are preferred leaders, no need to do anything"); } }
		 */
		
		// reassign partitions to new broker
		//Arrays contain the brokers to which partitions will be assigned
		Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = new HashMap<>();
		reassignment.put(new TopicPartition(TOPIC_NAME, 0),
				Optional.of(new NewPartitionReassignment(Arrays.asList(1,3,0))));
		reassignment.put(new TopicPartition(TOPIC_NAME, 1),
				Optional.of(new NewPartitionReassignment(Arrays.asList(0,3,1))));
		reassignment.put(new TopicPartition(TOPIC_NAME, 2),
				Optional.of(new NewPartitionReassignment(Arrays.asList(0,3,1))));
		reassignment.put(new TopicPartition(TOPIC_NAME, 3),
				Optional.of(new NewPartitionReassignment(Arrays.asList(1,3,0))));
		reassignment.put(new TopicPartition(TOPIC_NAME, 4), Optional.empty());

		try {
			admin.alterPartitionReassignments(reassignment).all().get();
		} catch (ExecutionException e) {
			if (e.getCause() instanceof NoReassignmentInProgressException) {
				System.out
						.println("We tried cancelling a reassignment that was not happening anyway. Lets ignore this.");
			}
		}

		System.out.println("currently reassigning: " + admin.listPartitionReassignments().reassignments().get());
		admin.close(Duration.ofSeconds(30));

	}
}
