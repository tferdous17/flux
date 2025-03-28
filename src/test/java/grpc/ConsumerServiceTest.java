package grpc;

import broker.Broker;
import consumer.ConsumerRecord;
import consumer.FluxConsumer;
import consumer.PollResult;
import org.junit.jupiter.api.Test;
import producer.FluxProducer;
import producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.List;

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

    private static void startClient() {
        FluxProducer<String, String> producer = new FluxProducer<>();
        FluxConsumer<String, String> consumer = new FluxConsumer<>();

        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-value");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("lsjkdfsadfkljopic234", "fvaluasdfae");
        ProducerRecord<String, String> record3 = new ProducerRecord<>("tasdlfhjwoeihjfsd", "34tgrvaadfasdflue");

        try {
            producer.sendDirect(record);
            producer.sendDirect(record2);
            producer.sendDirect(record3);

            // refer to KafkaConsumer for why the test is structured like this
            // note: kafka does not have a defined way to stop polling, but flux does for convenience purposes
            while (true) {
                PollResult result = consumer.poll(Duration.ofMillis(100));
                if (!result.shouldContinuePolling()) {
                    break;
                }
                for (ConsumerRecord<String, String> rec : result.records()) {
                    System.out.println(rec);
                }
            }


        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
