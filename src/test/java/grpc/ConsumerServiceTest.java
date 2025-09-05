package grpc;

import server.internal.Broker;
import consumer.ConsumerRecord;
import consumer.FluxConsumer;
import consumer.PollResult;
import org.junit.jupiter.api.Test;
import producer.FluxProducer;
import producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;

public class ConsumerServiceTest {
    @Test
    public void testConsumerService() {
        // start up server first
        // must put in a separate thread b/c its blocking
        Thread serverThread = new Thread(ConsumerServiceTest::startServer);
        serverThread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Start client
        clientPollTest();

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


    private static void warmup() {
        FluxProducer<String, String> producer = new FluxProducer<>();

        for (int i = 0; i < 100; i++) {
            String t = randStringGen();
            ProducerRecord<String, String> record = new ProducerRecord<>("topic", t);
            try {
                producer.send(record);
                Thread.sleep(1000);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

        producer.forceFlush();

    }

    private static void clientPollTest() {
        warmup();
        FluxConsumer<String, String> consumer = new FluxConsumer<>();
        consumer.subscribe(java.util.List.of("topic")); // Subscribe to the same topic the producer uses

        // refer to KafkaConsumer for why the test is structured like this
        // note: kafka does not have a defined way to stop polling, but flux does for convenience purposes
        while (true) {
            PollResult result = consumer.poll(Duration.ofMillis(100));
            if (!result.shouldContinuePolling()) {
                System.out.println("polling will no longer continue");
                break;
            }
            for (ConsumerRecord<String, String> rec : result.records()) {
                System.out.println(rec);
            }
        }
    }
}
