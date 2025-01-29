package clients;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
//import org.jline.builtins.telnet.Connection;

public class BasicProducer {
     public static void main(String[] args) {
	System.out.println("***Starting Basic Producer***");

        //configuration
        Properties settings = new Properties();
        settings.put("client.id", "basic-producer-v0.1.0");
        settings.put("bootstrap.servers", "c1:9092,c2:9093,c3:9092");
        settings.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        settings.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		
		/*
		 * setting.put(
		 * "max.block.ms,buffer.memory,compression.type,retries,delivery.timeout.ms" +
		 * "enable.idempotence","max.in.flight.requests.per.connection","batch.size",
		 * "Connection.max.idle.mx","delivery.timeout.ms","max.request.size"
		 */

        //create producer
        final KafkaProducer<String,String> producer = new KafkaProducer<>(settings);

       //shutdown behaviour
       Runtime.getRuntime().addShutdownHook(new Thread(() -> {
	   System.out.println("###Stopping Basic Producer###");
	   producer.close();
       }));

       final String topic = "Topic-new";
       for(int i=1; i<=5000; i++){
          final String key = "key-" + i;
          final String value = "value-" + i;
          final ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
	  producer.send(record);
       }
     }
}

