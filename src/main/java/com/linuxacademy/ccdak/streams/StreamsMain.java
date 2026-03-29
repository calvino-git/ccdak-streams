package com.linuxacademy.ccdak.streams;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class StreamsMain {

    public static void main(String[] args) {
        //Setup the configuration
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-example");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:9092");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        //get the source streams
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<String, String> kStream = streamsBuilder.stream("streams-input-topic");
        kStream.to("streams-output-topic");
        kStream.foreach((key, value) -> System.out.println(key + ":" + value));
        final Topology topology = streamsBuilder.build();
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        //Print the topology
        System.out.println(topology.describe());

        final CountDownLatch downLatch = new CountDownLatch(1);

        //Attach a shutdown handler
        Runtime.getRuntime().addShutdownHook(new Thread("streams-countdown-utdown-hook"){
            @Override
            public void run(){
                kafkaStreams.close();
                downLatch.countDown();
            }
        });

        try{
            kafkaStreams.start();
            downLatch.await();
        }catch(final Throwable t){
            System.out.println(t.getMessage());
            System.exit(1);
        }

    }

}
