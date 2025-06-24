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
        FluxProducer<String, String> producer = new FluxProducer<>(30, 60);
        while (true) {
            String t = randStringGen();
            ProducerRecord<String, String> record = new ProducerRecord<>("topic", t);
            try {
                producer.send(record);
                Thread.sleep(500);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }


}
