package newapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
//import org.apache.kafka.clients.admin.DescribeTopicsOptions;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicListing;

//import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ListingTopics {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
      Properties config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
      AdminClient admin = AdminClient.create(config);
      for (TopicListing topicListing : admin.listTopics().listings().get()) {
          System.out.println(topicListing);
      }
  }
}
