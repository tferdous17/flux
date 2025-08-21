package grpc;

import server.internal.Broker;
import org.junit.jupiter.api.Test;
import producer.FluxProducer;

import java.util.Properties;

public class MetadataServiceTest {

    @Test
    public void testOnlyMetadataCacheAutoRefreshing() {
        Thread serverThread = new Thread(MetadataServiceTest::startServer);
        serverThread.start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // This will print the initial metadata and then simply refresh (and log) the updated cache.
        // Change the interval inside FluxProducer--by default its 5 mins (change to 1 min for quicker testing).
        FluxProducer<String, String> producer = new FluxProducer<>(new Properties(), 300, 300);
        while (true) {}
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
}
