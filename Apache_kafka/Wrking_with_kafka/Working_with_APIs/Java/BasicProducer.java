package clients;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BasicProducer {
     public static void main(String[] args) {
	System.out.println("***Starting Basic Producer***");

        //configuration
        Properties settings = new Properties();
        settings.put("client.id", "basic-producer-v0.1.0");
        settings.put("bootstrap.servers", "c1:9092,c2:9093,c3:9092");
        settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        settings.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //create producer
        final KafkaProducer<String,String> producer = new KafkaProducer<>(settings);

       //shutdown behaviour
       Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	   System.out.println("###Stopping Basic Producer###");
	   producer.close();
       }));

       final String topic = "hello_world_topic";
       for(int i=1; i<=5; i++){
          final String key = "key-" + i;
          final String value = "value-" + i;
          final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
	  producer.send(record);
       }
     }
}
