package grpc;

import server.internal.Broker;
import grpc.services.ConsumerServiceImpl;
import grpc.services.CreateTopicsServiceImpl;
import grpc.services.MetadataServiceImpl;
import grpc.services.ProducerServiceImpl;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer {

    private Server server;
    private final Broker broker;

    public BrokerServer(Broker broker) {
        this.broker = broker;
    }

    public void start(int port) throws IOException {

        ExecutorService executor = Executors.newFixedThreadPool(6);
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .executor(executor)
                .addService(new ProducerServiceImpl(this.broker))
                .addService(new ConsumerServiceImpl(this.broker))
                .addService(new CreateTopicsServiceImpl(this.broker))
                .addService(new MetadataServiceImpl(this.broker))
                .build()
                .start();

        System.out.printf("Server started @ %s:%d with broker ID = %s%n", broker.getHost(), port, broker.getBrokerId());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown
                // hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    BrokerServer.this.stop();
                } catch (InterruptedException e) {
                    if (server != null) {
                        server.shutdownNow();
                    }
                    e.printStackTrace(System.err);
                } finally {
                    executor.shutdown();
                }
                System.err.println("*** server shut down");
            }
        });
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

}
