package newapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
//import org.apache.kafka.clients.admin.DescribeTopicsOptions;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

//import java.util.ArrayList;
//import java.util.Collection;
import java.util.Collections;
//import java.util.HashMap;
import java.util.List;
//import java.util.Map;
//import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConfigCheck {
	public static final String TOPIC_NAME = "Topic-new";
    public static final String CONSUMER_GROUP = "Test-group";
    public static final List<String> TOPIC_LIST = Collections.singletonList(TOPIC_NAME);
    public static final List<String> CONSUMER_GRP_LIST = Collections.singletonList(CONSUMER_GROUP);
    public static final int NUM_PARTITIONS = 3;
    public static final short REP_FACTOR = 3;
    
  public static void main(String[] args) throws ExecutionException, InterruptedException {
      Properties config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "c1:9092");
      AdminClient admin = AdminClient.create(config);
      for (TopicListing topicListing : admin.listTopics().listings().get()) {
          System.out.println(topicListing);
      }
      ConfigResource configResource =
              new ConfigResource(ConfigResource.Type.TOPIC,TOPIC_NAME);
      DescribeConfigsResult configsResult =
              admin.describeConfigs(Collections.singleton(configResource));
      Config configs = configsResult.all().get().get(configResource);

      // print non-default configs
      configs.entries().stream().filter(
              entry -> !entry.isDefault()).forEach(System.out::println);


      // Check if topic is compacted
      ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
              TopicConfig.CLEANUP_POLICY_COMPACT);
      if (! configs.entries().contains(compaction)) {
          // if topic is not compacted, compact it
    	System.out.println("--Our Topic-- " + TOPIC_NAME + "  is not compacted, to compact uncomment below");
          //Collection<AlterConfigOp> configOp = new ArrayList<AlterConfigOp>();
          //configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));
          //Map<ConfigResource, Collection<AlterConfigOp>> alterConfigs = new HashMap<>();
          //alterConfigs.put(configResource, configOp);
          //admin.incrementalAlterConfigs(alterConfigs).all().get();
      } else {
          System.out.println("Topic " + TOPIC_NAME + " is compacted topic");
      }

  }
}
