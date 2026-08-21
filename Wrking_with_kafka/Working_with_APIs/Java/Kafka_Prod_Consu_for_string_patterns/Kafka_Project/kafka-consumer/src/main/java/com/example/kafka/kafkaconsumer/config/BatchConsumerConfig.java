package com.example.kafka.kafkaconsumer.config;

import com.example.kafka.kafkaconsumer.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * ADDITIONAL CONFIG — Batch Listener
 * =====================================
 * Demonstrates processing MULTIPLE records per @KafkaListener invocation,
 * instead of one record at a time.
 *
 * Why batch processing matters:
 *   Single-record processing: 1 DB call per record → 1000 records = 1000 DB calls
 *   Batch processing:         1 DB call per batch  → 1000 records = 1 bulk DB call
 *
 * This is the difference between a consumer that can handle 100 msg/sec
 * and one that can handle 10,000 msg/sec when downstream calls are the bottleneck.
 *
 * setBatchListener(true) changes the @KafkaListener method signature from
 * accepting a single OrderEvent to accepting a List<OrderEvent> (see
 * BatchOrderListener.java).
 */
@Configuration
public class BatchConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, OrderEvent> batchConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-batch-processing-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Fetch up to 50 records per poll() — this is the MAX batch size
        // the listener method will receive in one invocation.
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> batchKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> batchConsumerFactory) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
        factory.setConsumerFactory(batchConsumerFactory);

        // THE key setting: tells Spring to deliver records as a List<T>
        // to the @KafkaListener method instead of one record at a time.
        factory.setBatchListener(true);

        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(1);

        return factory;
    }
}
