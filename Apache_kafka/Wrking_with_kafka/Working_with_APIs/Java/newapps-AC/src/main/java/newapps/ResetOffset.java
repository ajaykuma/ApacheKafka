package newapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
//import org.apache.kafka.clients.admin.DescribeTopicsOptions;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
//import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
//import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ResetOffset {
    
    public static final String CONSUMER_GROUP = "ApplGroup";
    public static final List<String> CONSUMER_GRP_LIST = Collections.singletonList(CONSUMER_GROUP);
    
	public static void main(String[] args) throws ExecutionException, InterruptedException {
      Properties config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "c1:9092");
      AdminClient admin = AdminClient.create(config);
      
      // List consumer groups
            
      ListConsumerGroupsResult groupResult = admin.listConsumerGroups();
      java.util.Iterator<ConsumerGroupListing> groups = groupResult.all().get().iterator();
      System.out.println("\n" + "Groups recently used: " );
      while (groups.hasNext()) {
			String groupId = groups.next().groupId();
			System.out.println(groupId);
			}
      // Get offsets committed by the group
      Map<TopicPartition, OffsetAndMetadata> offsets =
              admin.listConsumerGroupOffsets(CONSUMER_GROUP)
                      .partitionsToOffsetAndMetadata().get();

      Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
      Map<TopicPartition, OffsetSpec> requestEarliestOffsets = new HashMap<>();
      Map<TopicPartition, OffsetSpec> requestOlderOffsets = new HashMap<>();
      DateTime resetTo = new DateTime().minusHours(2);
      // For all topics and partitions that have offsets committed by the group, get their latest offsets, 
      //earliest offsets and the offset for 2h ago. 
      //we are only populating the request for 2h old offsets, but not using them.
      // You can swap the use of "Earliest" in the `alterConsumerGroupOffset` example with the offsets 
      //from 2h ago
      for(TopicPartition tp: offsets.keySet()) {
          requestLatestOffsets.put(tp, OffsetSpec.latest());
          requestEarliestOffsets.put(tp, OffsetSpec.earliest());
          requestOlderOffsets.put(tp, OffsetSpec.forTimestamp(resetTo.getMillis()));
      }

      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
              admin.listOffsets(requestLatestOffsets).all().get();

      for (Map.Entry<TopicPartition, OffsetAndMetadata> e: offsets.entrySet()) {
          String topic = e.getKey().topic();
          int partition =  e.getKey().partition();
          long committedOffset = e.getValue().offset();
          long latestOffset = latestOffsets.get(e.getKey()).offset();
          System.out.println("\n" + "Consumer group " + CONSUMER_GROUP
                  + " has committed offset " + committedOffset
                  + " to topic " + topic + " partition " + partition
                  + "\n" + " The latest offset in the partition is, "
                  +  latestOffset + " so consumer group is "
                  + (latestOffset - committedOffset) + " records behind");
      }
      
      // Reset offsets to beginning of topic. You can try to reset to 2h ago 
      //too by using `requestOlderOffsets`
      
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
              admin.listOffsets(requestEarliestOffsets).all().get();

      Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
      for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e:
              earliestOffsets.entrySet()) {
          System.out.println("\n" + "Will reset topic-partition " + e.getKey() + " to offset " + e.getValue().offset());
          resetOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()));
      }

      try {
          admin.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get();
      } catch (ExecutionException e) {
          System.out.println("Failed to update the offsets committed by group " + CONSUMER_GROUP +
                  " with error " + e.getMessage());
          if (e.getCause() instanceof UnknownMemberIdException)
              System.out.println("Check if consumer group is still active.");
      }

        }

}
