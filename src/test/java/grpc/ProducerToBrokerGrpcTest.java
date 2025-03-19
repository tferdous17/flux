package grpc;

import broker.Broker;
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
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-value");

        try {
            producer.sendDirect(record);
            System.out.println("Message sent");
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }
}
