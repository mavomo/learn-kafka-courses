package io.confluent.developer.basic;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class BasicStreams {

    public static void main(String[] args) throws IOException {
        System.out.println("Starting BasicStreams application...");

        Properties streamsProps = new Properties();
        try (FileInputStream fis = new FileInputStream("src/main/resources/streams.properties")) {
            streamsProps.load(fis);
        }
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams");
        StreamsBuilder builder = new StreamsBuilder();
        final String inputTopic = streamsProps.getProperty("basic.input.topic");
        final String outputTopic = streamsProps.getProperty("basic.output.topic");

        final String orderNumberStart = "orderNumber-";
        // Using the StreamsBuilder from above, create a KStream with an input-topic
        // and a Consumed instance with the correct
        // Serdes for the key and value HINT: builder.stream and Serdes.String()
        KStream<String, String> firstStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        firstStream.peek((key, value) -> System.out.println("1.Incoming record - key " + key + " value " + value))
                // filter records by making sure they contain the orderNumberStart variable from above HINT: use filter
                .filter((key, value) -> value.contains(orderNumberStart))
                // map the value to a new string by removing the orderNumberStart portion HINT: use mapValues
                .mapValues(value -> value.substring(value.indexOf("-") + 1))
                // only forward records where the value is 1000 or greater HINT: use filter and Long.parseLong
                .filter((key, value) -> Long.parseLong(value) > 1000L)
                .peek((key, value) -> System.out.println("2.Outgoing record - key " + key + " value " + value))
                // Write the results to an output topic defined above as outputTopic HINT: use "to" and Produced and Serdes.String()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            // Add State listener
            kafkaStreams.setStateListener((newState, oldState) -> {
                System.out.println("State transition from " + oldState + " to " + newState);
            });

            kafkaStreams.setUncaughtExceptionHandler( uncaughtHandler -> {
                System.out.println("Streams application got an error: " + uncaughtHandler.getMessage());
                return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });

            final CountDownLatch shutdownLatch = new CountDownLatch(1);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.exit(1);
            }
        }
        System.exit(0);
    }
}

