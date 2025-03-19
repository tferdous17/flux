package grpc;

import broker.Broker;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import proto.BrokerToPublisherAck;
import proto.PublishDataToBrokerRequest;
import proto.PublishToBrokerGrpc;

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

        ExecutorService executor = Executors.newFixedThreadPool(2);
        server = Grpc.newServerBuilderForPort(port, InsecureServerCredentials.create())
                .executor(executor)
                .addService(new PublishToBrokerImpl(this.broker))
                .build()
                .start();

        System.out.println("Server started on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
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

    static class PublishToBrokerImpl extends PublishToBrokerGrpc.PublishToBrokerImplBase {
        Broker broker;

        public PublishToBrokerImpl(Broker broker) {
            this.broker = broker;
        }

        @Override
        public void send(PublishDataToBrokerRequest req, StreamObserver<BrokerToPublisherAck> responseObserver) {
            byte[] data = req.getData().toByteArray();

            try {
                broker.produceSingleMessage(data);
            } catch (IOException e) {
                responseObserver.onError(e);
            }

            BrokerToPublisherAck ack = BrokerToPublisherAck
                    .newBuilder()
                    .setAcknowledgement("DATA BEEN RECEIVED: " + req.getData())
                    .build();

            responseObserver.onNext(ack); // this just sends the response back to the client
            responseObserver.onCompleted(); // lets the client know there are no more messages after this
        }
    }

}
