package com.example.kafka.kafkaconsumer.config;

import com.example.kafka.kafkaconsumer.model.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * ADDITIONAL CONFIG — JSON Consumer with Manual Ack + Retry + DLT
 * ===================================================================
 * Your existing KafkaConfig.java uses auto-commit and plain String values.
 * This adds a production-grade consumer factory with:
 *
 *   1. JSON deserialization to OrderEvent POJO
 *   2. ErrorHandlingDeserializer — wraps the JSON deserializer so that a
 *      malformed record doesn't crash the listener container; the error
 *      is captured and routed through the error handler instead.
 *   3. Manual acknowledgment (AckMode.MANUAL) — you control exactly when
 *      the offset is committed, instead of Spring auto-committing after
 *      every poll batch.
 *   4. DefaultErrorHandler with retry + Dead Letter Topic — Spring's
 *      built-in equivalent of the DLT pattern in the plain Kafka examples.
 */
@EnableKafka
@Configuration
public class JsonConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    // ── Consumer factory: JSON values, error-handling wrapper ──────────────
    @Bean
    public ConsumerFactory<String, OrderEvent> orderConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // ErrorHandlingDeserializer wraps the real deserializer. If JSON
        // parsing fails, it returns a DeserializationException as the value
        // instead of throwing — this lets the error handler (below) catch
        // it cleanly and route to DLT, rather than crashing the consumer thread.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);

        // Tell JsonDeserializer what type to deserialize into.
        // Combined with ADD_TYPE_INFO_HEADERS=false on the producer side,
        // this means: "ignore any type header, always deserialize as OrderEvent"
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, OrderEvent.class.getName());
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-processing-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // CRITICAL: disable auto-commit. We commit manually via Acknowledgment
        // (see OrderEventListener) only AFTER successful processing.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(props);
    }

    // ── DLT producer factory ────────────────────────────────────────────────
    // Spring's DeadLetterPublishingRecoverer needs its own KafkaTemplate to
    // write failed records to a "<topic>.DLT" topic automatically.
    @Bean
    public ProducerFactory<String, Object> dltProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> dltKafkaTemplate() {
        return new KafkaTemplate<>(dltProducerFactory());
    }

    // ── Error Handler: Retry 3 times, then route to DLT ─────────────────────
    // This is Spring's built-in equivalent of the manual try/catch + DLT
    // pattern used in the plain Kafka examples (IngestionPipeline, etc).
    //
    // Flow on processing failure:
    //   1. Exception thrown in @KafkaListener method
    //   2. DefaultErrorHandler catches it
    //   3. Retries the SAME record up to 3 times, 1 second apart
    //      (FixedBackOff(1000L, 3L) = 1000ms interval, 3 retries)
    //   4. If still failing after 3 retries → DeadLetterPublishingRecoverer
    //      sends the record to "<original-topic>.DLT"
    //   5. Offset is committed either way — processing moves forward
    @Bean
    public CommonErrorHandler orderErrorHandler(KafkaTemplate<String, Object> dltKafkaTemplate) {
        var recoverer = new DeadLetterPublishingRecoverer(dltKafkaTemplate,
                (record, exception) -> {
                    // Determines the DLT topic name and partition.
                    // Default Spring behaviour: "<topic>.DLT", same partition count.
                    return new org.apache.kafka.common.TopicPartition(
                            record.topic() + ".DLT", record.partition());
                });

        // FixedBackOff(interval_ms, max_retries)
        var backOff = new FixedBackOff(1000L, 3L);

        var errorHandler = new DefaultErrorHandler(recoverer, backOff);

        // Don't retry for these exception types — they will NEVER succeed
        // on retry (e.g. a permanently malformed record). Send straight to DLT.
        errorHandler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class);

        return errorHandler;
    }

    // ── Listener container factory with manual ack + error handler ─────────
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> orderKafkaListenerContainerFactory(
            ConsumerFactory<String, OrderEvent> orderConsumerFactory,
            CommonErrorHandler orderErrorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderEvent>();
        factory.setConsumerFactory(orderConsumerFactory);
        factory.setCommonErrorHandler(orderErrorHandler);

        // AckMode.MANUAL: the listener method must call acknowledgment.acknowledge()
        // explicitly. Spring will NOT auto-commit after the method returns.
        // This gives you full control — e.g. only ack after a successful DB write.
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // Number of concurrent consumer threads in this container.
        // Cannot exceed the topic's partition count (excess threads idle).
        factory.setConcurrency(1);

        return factory;
    }
}
