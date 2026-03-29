package com.linuxacademy.ccdak.streams;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

public class StatelessTransformationsMain {

    public static void main(String[] args) {
        // Set up the configuration.
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stateless-transformations-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        // Since the input topic uses Strings for both key and value, set the default Serdes to String.
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Get the source stream.
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> kStream = builder.stream("stateless-transformation-input-topic");
        
        //Split into 2 streams
        Predicate<String, String> p1 = (key, value) -> value.charAt(0) > 'A';
        Predicate<String, String> p2 = (key, value) -> true;
        KStream<String,String>[] branches = kStream.branch(p1, p2);
        KStream<String,String> multiOf10Stream = branches[0];
        KStream<String,String> otherStream = branches[1];

        KStream<String,String> fullStream = multiOf10Stream.filter((key, value) -> true)
        .flatMap((key, value) -> {
            List<KeyValue<String, String>> result = new LinkedList<>();
            result.add(KeyValue.pair(key, value + ":" + String.valueOf((char) Integer.valueOf(key).intValue()).toLowerCase()));
            result.add(KeyValue.pair(key, value + ":" + String.valueOf((char) Integer.valueOf(key).intValue()).toLowerCase()));
            return result;
        })
        .map((key, value) -> KeyValue.pair(key, value))
        .merge(otherStream);
        
        //multiOf10Stream.peek((key, value) -> System.out.println(key + ":" + value));

        fullStream.peek((key, value) -> System.out.println(key + ":" + value));

        fullStream.to("stateless-transformation-output-topic");

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        // Print the topology to the console.
        System.out.println(topology.describe());
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach a shutdown handler to catch control-c and terminate the application gracefully.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
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
            System.out.println(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }

}
