package newapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class CheckandCreateTopic {
	
    public static final String TOPIC_NAME = "Topic-new";
    public static final String CONSUMER_GROUP = "Test-group";
    public static final List<String> TOPIC_LIST = Collections.singletonList(TOPIC_NAME);
    public static final List<String> CONSUMER_GRP_LIST = Collections.singletonList(CONSUMER_GROUP);
    public static final int NUM_PARTITIONS = 3;
    public static final short REP_FACTOR = 1;
    
  public static void main(String[] args) throws ExecutionException, InterruptedException {
      Properties config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      
      AdminClient admin = AdminClient.create(config);
      TopicDescription topicDescription;

      //listing
      System.out.println("-- listing --");
      admin.listTopics().names().get().forEach(System.out::println);
      
   // check if our demo topic exists, create it if it doesn't
      DescribeTopicsResult ourTopic = admin.describeTopics(TOPIC_LIST);
      try {
          topicDescription = ourTopic.topicNameValues().get(TOPIC_NAME).get();
          System.out.println("Topic exists");
          
          System.out.println("Description of our topic:" + '\n' + topicDescription);
          System.out.println("Partition Size: " + topicDescription.partitions().size() );

          if (topicDescription.partitions().size() != NUM_PARTITIONS) {
              System.out.println("Topic has wrong number of partitions.");
              //System.exit(-1);
          }
      } catch (ExecutionException e) {
          // exit early for almost all exceptions
          if (! (e.getCause() instanceof UnknownTopicOrPartitionException)) {
              e.printStackTrace();
              throw e;
          }

          // if we are here, topic doesn't exist
          System.out.println("Topic " + TOPIC_NAME +
                  " does not exist. Going to create it now");
          // Note that number of partitions and replicas are optional. If there are
          // not specified, the defaults configured on the Kafka brokers will be used
          CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(
                  new NewTopic(TOPIC_NAME, NUM_PARTITIONS, REP_FACTOR)));

          // uncomment to see the topic get created with wrong number of partitions
          //CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(new NewTopic(TOPIC_NAME, Optional.empty(), Optional.empty())));

          // Check that the topic was created correctly:
          if (newTopic.numPartitions(TOPIC_NAME).get() != NUM_PARTITIONS) {
              System.out.println("Topic was created with wrong number of partitions. Exiting.");
              // System.exit(-1);
          }
      }


  }
}
