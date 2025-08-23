package grpc;

import grpc.services.*;
import server.internal.Broker;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer {

    private Server server;
    private final Broker broker;
    private final ExecutorService executor;

    public BrokerServer(Broker broker) {
        executor = Executors.newFixedThreadPool(6);
        this.broker = broker;
    }

    public void start(int port) throws IOException {
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .executor(executor)
                .addService(new ProducerServiceImpl(this.broker))
                .addService(new ConsumerServiceImpl(this.broker))
                .addService(new CreateTopicsServiceImpl(this.broker))
                .addService(new MetadataServiceImpl(this.broker))
                .addService(new ControllerServiceImpl(this.broker))
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
        executor.shutdown();
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }
}
