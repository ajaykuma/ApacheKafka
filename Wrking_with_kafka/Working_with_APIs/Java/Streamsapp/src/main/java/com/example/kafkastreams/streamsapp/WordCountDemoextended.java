/*
 * The changelog mirrors your KTable state
 --Read change log directly
 kafka-console-consumer.bat --bootstrap-server localhost:9092 ^
    --topic streams-wordcount-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog ^
    --from-beginning ^
    --formatter kafka.tools.DefaultMessageFormatter ^
    --property print.key=true ^
    --property print.value=true ^
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 
 *
 Use ksqldb
 # Download from:
https://ksqldb.io/quickstart.html
--instructions in 'Instructions_for_ksql.txt'

 *
 kafka-console-consumer.bat --bootstrap-server localhost:9092 ^
    --topic streams-wordcount-output ^
    --from-beginning ^
    --formatter kafka.tools.DefaultMessageFormatter ^
    --property print.key=true ^
    --property print.value=true ^
    --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer ^
    --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 */
package com.example.kafkastreams.streamsapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class WordCountDemoextended {

    public static final String INPUT_TOPIC  = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";
    public static final String STORE_NAME   = "word-count-store";

    static Properties getStreamsConfig(final String[] args) throws IOException {
        final Properties props = new Properties();
        if (args != null && args.length > 0) {
            try (final FileInputStream fis = new FileInputStream(args[0])) {
                props.load(fis);
            }
            if (args.length > 1) {
                System.out.println("Warning: Some command line arguments were ignored.");
            }
        }
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG,              "streams-wordcount");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,           "localhost:9092");
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,     Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,   Serdes.String().getClass().getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,          "earliest");
        return props;
    }

    //Materialized with a store name only if you want both ksqlDB and queryStore()
    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        final KTable<String, Long> counts = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        counts.toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    static void queryStore(final KafkaStreams streams) {
        // Initial wait for stream to be in RUNNING state
        try { Thread.sleep(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }

        ReadOnlyKeyValueStore<String, Long> store = streams.store(
            StoreQueryParameters.fromNameAndType(STORE_NAME, QueryableStoreTypes.keyValueStore())
        );

        while (!Thread.currentThread().isInterrupted()) {
            System.out.println("\n========= KTable State Store Query =========");

            long count = 0;
            KeyValueIterator<String, Long> iterator = store.all();
            while (iterator.hasNext()) {
                KeyValue<String, Long> kv = iterator.next();
                System.out.println("Word: " + kv.key + "  |  Count: " + kv.value);
                count++;
            }
            iterator.close(); // always close iterator to avoid resource leak

            if (count == 0) {
                System.out.println("No data in store yet — waiting for words...");
            }
            System.out.println("============================================\n");

            try { Thread.sleep(30000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); break; }
        }
    }

    //or simplified app just for ksqlDB, without queryStore
    /*
    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count()                          // no Materialized needed
            .toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    } 
    */
    
    public static void main(final String[] args) throws IOException {
        final Properties props   = getStreamsConfig(args);
        final StreamsBuilder builder = new StreamsBuilder();
        createWordCountStream(builder);

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();

            new Thread(() -> queryStore(streams), "store-query-thread").start();

            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}