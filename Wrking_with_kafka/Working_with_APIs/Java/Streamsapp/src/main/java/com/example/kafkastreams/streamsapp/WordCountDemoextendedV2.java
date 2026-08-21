/*
 * --changes from the original WordCountDemoextended:
 *  - Output value is now JSON-encoded (a small {"WORD_COUNT": <long>} object)
 *    instead of raw Serdes.Long() bytes.
 *  - This lets ksqlDB use VALUE_FORMAT='JSON' on the output topic, which
 *    supports CREATE TABLE AS SELECT, push queries, and pull queries.
 *    The raw KAFKA format only supports a single bare field and breaks
 *    as soon as you try to derive/materialize a table from it.
 *
 * Matching ksqlDB DDL for this version:
 *
 *   DROP TABLE word_counts;
 *
 *   CREATE TABLE word_counts (
 *       WORD VARCHAR PRIMARY KEY,
 *       WORD_COUNT BIGINT
 *   ) WITH (
 *       KAFKA_TOPIC='streams-wordcount-output',
 *       VALUE_FORMAT='JSON',
 *       KEY_FORMAT='KAFKA'
 *   );
 *
 *   CREATE TABLE queryable_word_counts AS
 *     SELECT WORD, WORD_COUNT
 *     FROM word_counts;
 *
 *   SELECT WORD, WORD_COUNT FROM queryable_word_counts EMIT CHANGES;
 *   SELECT * FROM queryable_word_counts WHERE WORD = 'hello';
 *
 * NOTE: Before running this version, delete the old output topic so it
 * doesn't contain a mix of raw-long and JSON-encoded records:
 *
 *   kafka-topics.sh --bootstrap-server localhost:9092 \
 *       --delete --topic streams-wordcount-output
 *
 * Read changelog directly (unchanged from v1):
 *   kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 *      --topic streams-wordcount-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog \
 *      --from-beginning \
 *      --formatter kafka.tools.DefaultMessageFormatter \
 *      --property print.key=true \
 *      --property print.value=true \
 *      --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 *      --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer
 *
 * Read output topic directly with the new JSON value (v2):
 *   kafka-console-consumer.sh --bootstrap-server localhost:9092 \
 *      --topic streams-wordcount-output \
 *      --from-beginning \
 *      --property print.key=true \
 *      --property print.value=true \
 *      --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
 *      --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 */
package com.example.kafkastreams.streamsapp;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
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
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class WordCountDemoextendedV2 {

    public static final String INPUT_TOPIC  = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";
    public static final String STORE_NAME   = "word-count-store";

    // Key under which the count is nested in the JSON value, e.g. {"WORD_COUNT": 5}
    // Must match the column name used in the ksqlDB CREATE TABLE statement.
    private static final String JSON_FIELD_NAME = "WORD_COUNT";

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

    /**
     * A Serde that encodes/decodes a Long as a small JSON object,
     * e.g. {"WORD_COUNT": 5}, so ksqlDB can map it onto a named BIGINT column
     * using VALUE_FORMAT='JSON'.
     */
    static Serde<Long> jsonLongValueSerde() {
        final ObjectMapper mapper = new ObjectMapper();

        final Serializer<Long> serializer = (topic, data) -> {
            try {
                if (data == null) {
                    return null;
                }
                final Map<String, Long> wrapped = Collections.singletonMap(JSON_FIELD_NAME, data);
                return mapper.writeValueAsBytes(wrapped);
            } catch (Exception e) {
                throw new RuntimeException("Error serializing Long to JSON", e);
            }
        };

        final Deserializer<Long> deserializer = (topic, bytes) -> {
            try {
                if (bytes == null) {
                    return null;
                }
                @SuppressWarnings("unchecked")
                final Map<String, Object> m = mapper.readValue(bytes, Map.class);
                final Object value = m.get(JSON_FIELD_NAME);
                return value == null ? null : ((Number) value).longValue();
            } catch (Exception e) {
                throw new RuntimeException("Error deserializing JSON to Long", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }

    // Materialized with a store name only if you want both ksqlDB and queryStore()
    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        final KTable<String, Long> counts = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(STORE_NAME)
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        // Value is JSON-encoded on the way out so ksqlDB can read it with VALUE_FORMAT='JSON'.
        counts.toStream()
            .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), jsonLongValueSerde()));
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
