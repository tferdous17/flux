package grpc;

import broker.Broker;
import com.google.protobuf.ByteString;
import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import org.tinylog.Logger;
import proto.*;

import java.io.IOException;
import java.util.List;
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
                .addService(new ConsumerServiceImpl(this.broker))
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
            BrokerToPublisherAck.Builder ackBuilder = BrokerToPublisherAck.newBuilder();
            List<byte[]> data = req
                                .getDataList()
                                .stream()
                                .map(ByteString::toByteArray)
                                .toList();

            System.out.println("DATA LIST: " + data);
            try {
                Logger.info("Producing messages");
                int recordOffset = broker.produceMessages(data);
                ackBuilder
                    .setAcknowledgement("ACK: Data received successfully.")
                    .setStatus(Status.SUCCESS)
                    .setRecordOffset(recordOffset);

            } catch (IOException e) {
                // will need logic in the future to differentiate between transient and permanent failures
                // producer will need to explicitly handle these failures and possibly retry
                 ackBuilder
                        .setAcknowledgement("ERR: " + e.getMessage())
                        .setStatus(Status.TRANSIENT_FAILURE)
                        .setRecordOffset(-1);
            }

            responseObserver.onNext(ackBuilder.build()); // this just sends the response back to the client
            responseObserver.onCompleted(); // lets the client know there are no more messages after this
        }
    }

    static class ConsumerServiceImpl extends ConsumerServiceGrpc.ConsumerServiceImplBase {
        Broker broker;
        int nextOffset; // to read

        public ConsumerServiceImpl(Broker broker) {
            this.broker = broker;
        }

        @Override
        public void fetchMessage(FetchMessageRequest req, StreamObserver<FetchMessageResponse> responseObserver) {
            FetchMessageResponse.Builder responseBuilder = FetchMessageResponse.newBuilder();
            nextOffset = req.getStartingOffset() + 1;
            try {
                Message msg = this.broker.consumeMessage(req.getStartingOffset());
                if (msg != null) {
                    responseBuilder
                            .setMessage(msg)
                            .setStatus(Status.SUCCESS)
                            .setNextOffset(nextOffset);
                    nextOffset++;
                } else {
                    // no more messages to read OR data not yet flushed to disk
                    responseBuilder
                            .setMessage(Message.newBuilder().getDefaultInstanceForType())
                            .setStatus(Status.READ_COMPLETION)
                            .setNextOffset(nextOffset);
                }
            } catch (IOException e) {
                responseObserver.onError(e);
            }

            responseObserver.onNext(responseBuilder.build()); // this just sends the response back to the client
            responseObserver.onCompleted(); // lets the client know there are no more messages after this

        }
    }
}
