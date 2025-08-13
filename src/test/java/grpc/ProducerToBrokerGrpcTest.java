package grpc;

import admin.NewTopic;
import server.internal.Broker;
import org.junit.jupiter.api.Test;
import producer.FluxProducer;
import producer.ProducerRecord;

import java.io.IOException;

public class ProducerToBrokerGrpcTest {

    @Test
    public void testProducerToBrokerGrpcFlow() {
        // start up server first
        // must put in a separate thread b/c its blocking
        Thread serverThread = new Thread(ProducerToBrokerGrpcTest::startServer);
        serverThread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // this is the address to the broker
//        Admin admin = FluxAdminClient.create(Set.of(new InetSocketAddress("localhost", 50051))); // bootstrap server
        NewTopic topic = new NewTopic("test-topic", 3, 1);
//        admin.createTopics(List.of(topic));

        // Start client
        startClient();
    }

    private static void startServer() {
        try {
            Broker broker = new Broker();
            BrokerServer server = new BrokerServer(broker);
            server.start(50051);
            server.blockUntilShutdown();

            System.out.println("Server started at port 50051");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    private static String randStringGen() {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < 50; i++) {
            sb.append(chars.charAt((int) (Math.random() * chars.length())));
        }
        return sb.toString();
    }

    // Make sure to manually close when done with testing this
    private static void startClient() {
        FluxProducer<String, String> producer = new FluxProducer<>(15, 60);
        while (true) {
            String t = randStringGen();

            // ! UNCOMMENT EACH RECORD BELOW TO TEST EACH SCENARIO FOR PARTITION SELECTION
            // Note, with the topic creation in testProducerToBrokerGrpcFlow(), the topic's partitions id range from [2,4] inclusive
            // Topic 1 is the default partition as set by default constructor for the Broker class.

            // Valid topic, partition #, and key. Should end up in the designated partition.
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", 2, t, "test-value");

            // Valid topic and key, but no partition #. Should result in key-based hashing among topic's partitions.
//            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", t, "test-value");

            // Valid topic, but invalid partition #. Should resort to key-based hashing.
//            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", -5, t, "test-value");

            // Valid topic, but no partition # or key. Partition selection should default to round-robin among topic's partitions.
//            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", t);

            // Invalid topic. Should immediately throw runtime exception.
//            ProducerRecord<String, String> record = new ProducerRecord<>("nonexistenttopic", 2, t, "test-value");
            try {
                producer.send(record);
                Thread.sleep(500);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
