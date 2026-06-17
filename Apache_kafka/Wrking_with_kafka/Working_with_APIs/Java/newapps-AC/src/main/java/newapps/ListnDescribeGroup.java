package newapps;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
//import org.apache.kafka.clients.admin.DescribeTopicsOptions;
//import org.apache.kafka.clients.admin.DescribeTopicsResult;
//import org.apache.kafka.clients.admin.TopicListing;

import java.util.Collections;
import java.util.List;
//import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ListnDescribeGroup {
    
    public static final String CONSUMER_GROUP = "Test-group";
    public static final List<String> CONSUMER_GRP_LIST = Collections.singletonList(CONSUMER_GROUP);
    
	public static void main(String[] args) throws ExecutionException, InterruptedException {
      Properties config = new Properties();
      config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "c1:9092");
      AdminClient admin = AdminClient.create(config);
      
      // List consumer groups
      System.out.println("Listing consumer groups with defaults, if any exist:");
      admin.listConsumerGroups().valid().get().forEach(System.out::println);

      // Describe a group
      ConsumerGroupDescription groupDescription = admin
              .describeConsumerGroups(CONSUMER_GRP_LIST)
              .describedGroups().get(CONSUMER_GROUP).get();
      
      System.out.println("\n" + "Description of selected " + CONSUMER_GROUP
              + ":" +"\n" + groupDescription );
      
      ListConsumerGroupsResult groupResult = admin.listConsumerGroups();
      java.util.Iterator<ConsumerGroupListing> groups = groupResult.all().get().iterator();
      System.out.println("\n" + "Groups recently used: " );
      while (groups.hasNext()) {
			String groupId = groups.next().groupId();
			System.out.println(groupId);
			}

  }

}
