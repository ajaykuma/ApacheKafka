//Refer: Testing_n_working_cluster3_strm.txt
package com.example.kafkastreams.streamsapp;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Suppressed;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class WordCountDemo {

    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";

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
        props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Removed CACHE_MAX_BYTES_BUFFERING_CONFIG — deleted in Kafka 3.x
        props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

        // Updated windowing API for Kafka Streams 3.x
        Duration windowSize    = Duration.ofMinutes(1);
        //Duration advanceSize   = Duration.ofMinutes(1);
        Duration advanceSize = Duration.ofSeconds(30);
        Duration inactivityGap = Duration.ofMinutes(5);
        Duration gracePeriod   = Duration.ofMillis(500);
        Duration timeDifference = Duration.ofSeconds(2);

        // New API: ofSizeWithNoGrace() or ofSizeAndGrace()
        //TimeWindows tumblingWindow = TimeWindows.ofSizeWithNoGrace(windowSize);
        //hopping window definition — advanceBy should be less than windowSize for it to actually "hop".
        TimeWindows hoppingWindow  = TimeWindows.ofSizeAndGrace(windowSize, gracePeriod).advanceBy(advanceSize);

        final KTable<Windowed<String>, Long> counts = source
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
            .groupBy((key, value) -> value)
            //.windowedBy(tumblingWindow)
            // Uncomment one windowing strategy as needed:
            .windowedBy(hoppingWindow)
            //.windowedBy(SessionWindows.ofInactivityGapWithNoGrace(inactivityGap))
            //.windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(timeDifference, gracePeriod))
            //.count();
            //when using suppress uncomment this
            .count(Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("word-count-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));
        
        /*When no windowing*
         & Consumer: StringDeserializer for key * /
        //counts.toStream()
        //.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        
        /*When using tumbling window: simpler output
         Consumer: StringDeserializer for key
         */
        counts
        //to only see final count per window (after window time),we can uncomment the suppress
        //.suppress(Suppressed.untilWindowCloses(
        //        Suppressed.BufferConfig.unbounded()))
        .toStream()
        .map((windowed, count) -> new KeyValue<>(windowed.key(), count))
        .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        
        /*When using tumbling window: richer output
         Consumer: TimeWindowedDeserializer with window.size.ms=60000
         */
        //counts.toStream()
        //.to(OUTPUT_TOPIC, Produced.with(
        //    WindowedSerdes.timeWindowedSerdeFrom(String.class, 60000L),
        //    Serdes.Long()
        //));
        
        /*When using hopping window - since overlapping windows
         * Consumer: TimeWindowedDeserializer with window.size.ms=60000
         * --to be checked for newer versions
          */
        //counts.toStream()
        //.to(OUTPUT_TOPIC, Produced.with(
        //    WindowedSerdes.timeWindowedSerdeFrom(String.class, 60000L),
        //    Serdes.Long()
        //));
       
        
    }

    public static void main(final String[] args) throws IOException {
        final Properties props = getStreamsConfig(args);
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
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}