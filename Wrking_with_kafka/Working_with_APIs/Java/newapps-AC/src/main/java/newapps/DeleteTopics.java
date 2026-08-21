package newapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.apache.kafka.clients.admin.AlterConfigOp;
//import org.apache.kafka.clients.admin.Config;
//import org.apache.kafka.clients.admin.ConfigEntry;
//import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
//import org.apache.kafka.clients.admin.DescribeTopicsOptions;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;
//import org.apache.kafka.common.config.ConfigResource;
//import org.apache.kafka.common.config.TopicConfig;

//import java.util.ArrayList;
//import java.util.Collection;
import java.util.Collections;
//import java.util.HashMap;
import java.util.List;
//import java.util.Map;
//import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DeleteTopics {
	public static final String TOPIC_NAME = "Topic-new";
    public static final String CONSUMER_GROUP = "Test-group";
    public static final List<String> TOPIC_LIST = Collections.singletonList(TOPIC_NAME);
    
    
  public static void main(String[] args) throws ExecutionException, InterruptedException {
      Properties config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      AdminClient admin = AdminClient.create(config);
      for (TopicListing topicListing : admin.listTopics().listings().get()) {
          System.out.println(topicListing);
      }
      //deleting topic
      admin.deleteTopics(TOPIC_LIST).all().get();

      // Check that it is gone. Note that due to the async nature of deletes,
      // it is possible that at this point the topic still exists
      DescribeTopicsResult OurTopic = admin.describeTopics(TOPIC_LIST);
      try {
    	    OurTopic.topicNameValues().get(TOPIC_NAME).get();
    	    System.out.println("Topic " + TOPIC_NAME + " is still around");
    	} catch (ExecutionException e) {
    	    System.out.println("Topic " + TOPIC_NAME + " is gone");
    	}


  }
}
