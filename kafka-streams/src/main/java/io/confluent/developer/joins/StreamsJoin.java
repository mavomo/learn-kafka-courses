package io.confluent.developer.joins;

import io.confluent.developer.StreamsUtils;
import io.confluent.developer.avro.ApplianceOrder;
import io.confluent.developer.avro.CombinedOrder;
import io.confluent.developer.avro.ElectronicOrder;
import io.confluent.developer.avro.User;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamsJoin {

    static <T extends SpecificRecord> SpecificAvroSerde<T> getSpecificAvroSerde(final Map<String, Object> serdeConfig) {
        final SpecificAvroSerde<T> specificAvroSerde = new SpecificAvroSerde<>();
        specificAvroSerde.configure(serdeConfig, false);
        return specificAvroSerde;
    }

    public static void main(String[] args) throws IOException {
        Properties streamsProps = StreamsUtils.loadProperties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "joining-streams");

        StreamsBuilder builder = new StreamsBuilder();
        String streamOneInput = streamsProps.getProperty("stream_one.input.topic");
        String streamTwoInput = streamsProps.getProperty("stream_two.input.topic");
        String tableInput = streamsProps.getProperty("table.input.topic");
        String outputTopic = streamsProps.getProperty("joins.output.topic");

        Map<String, Object> configMap = StreamsUtils.propertiesToMap(streamsProps);

        SpecificAvroSerde<ApplianceOrder> applianceSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<ElectronicOrder> electronicSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<CombinedOrder> combinedSerde = getSpecificAvroSerde(configMap);
        SpecificAvroSerde<User> userSerde = getSpecificAvroSerde(configMap);

        ValueJoiner<ApplianceOrder, ElectronicOrder, CombinedOrder> orderJoiner =
                (applianceOrder, electronicOrder) -> CombinedOrder.newBuilder()
                        .setApplianceOrderId(applianceOrder.getOrderId())
                        .setApplianceId(applianceOrder.getApplianceId())
                        .setElectronicOrderId(electronicOrder.getOrderId())
                        .setTime(Instant.now().toEpochMilli())
                        .build();
        System.out.println("1. Debug - After ValueJoiner");
        ValueJoiner<CombinedOrder, User, CombinedOrder> enrichmentJoiner = (combined, user) -> {
            if (user != null) {
                combined.setUserName(user.getName());
            }
            return combined;
        };
        System.out.println("2. Debug - After EnrichmentJoiner");

        KStream<String, ApplianceOrder> applianceStream = builder.stream(streamOneInput, Consumed.with(Serdes.String(), applianceSerde))
                .peek((key, value) -> System.out.println("Appliance stream incoming record key " + key + " value " + value));

        System.out.println("3. Debug - After applianceStream");

        KStream<String, ElectronicOrder> electronicStream = builder.stream(streamTwoInput, Consumed.with(Serdes.String(), electronicSerde))
                .peek((key, value) -> System.out.println("Electronic stream incoming record " + key + " value " + value));

        System.out.println("4. Debug - After electronicStream");

        KTable<String, User> userTable = builder.table(tableInput, Materialized.with(Serdes.String(), userSerde));
        System.out.println("5. Debug - After userTable");

        KStream<String, CombinedOrder> combinedStream = applianceStream.join(electronicStream,
                        // using the ValueJoiner created above, orderJoiner gets you the correct value type of CombinedOrder
                        orderJoiner,
                        // You want to join records within 30 minutes of each other HINT: JoinWindows and Duration.ofMinutes
                        JoinWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(30), Duration.ofMinutes(10)),
                        StreamJoined.with(Serdes.String(), applianceSerde, electronicSerde))
                // Optionally add this statement after the join to see the results on the console
                .peek((key, value) -> System.out.println("Stream-Stream Join record key " + key + " value " + value));
        System.out.println("6. Debug - After combinedStream");


        // Now join the combinedStream with the userTable,
        combinedStream.leftJoin(
                        userTable,
                        // but you'll always want a result even if no corresponding entry is found in the table
                        // Using the ValueJoiner created above, enrichmentJoiner, return a CombinedOrder instance enriched with user information
                        enrichmentJoiner,
                        // You'll need to add a Joined instance with the correct Serdes for the join state store
                        Joined.with(Serdes.String(), combinedSerde, userSerde))
                // Add these two statements after the join call to print results to the console and write results out
                .peek((key, value) -> System.out.println("Stream-Table Join record key " + key + " value " + value))
                // to a topic
                .to(outputTopic, Produced.with(Serdes.String(), combinedSerde));

        try (KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsProps)) {
            final CountDownLatch shutdownLatch = new CountDownLatch(1);
            kafkaStreams.setStateListener((newState, oldState) -> {
                System.out.println("[StreamJoins] - State transition from " + oldState + " to " + newState);
            });

            kafkaStreams.setUncaughtExceptionHandler( uncaughtHandler -> {
                System.out.println("[StreamJoins] application got an error: " + uncaughtHandler.getMessage());
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
            });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                kafkaStreams.close(Duration.ofSeconds(2));
                shutdownLatch.countDown();
            }));
            TopicLoader.runProducer();
            try {
                kafkaStreams.start();
                shutdownLatch.await();
            } catch (Throwable e) {
                System.out.println("Error on starting the app --- " + e);
                System.exit(1);
            }
        }
        System.exit(0);
    }
}
