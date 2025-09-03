package grpc.services;

import server.internal.Broker;
import io.grpc.stub.StreamObserver;
import proto.*;

import java.io.IOException;

public class ConsumerServiceImpl extends ConsumerServiceGrpc.ConsumerServiceImplBase {
    Broker broker;

    public ConsumerServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void fetchMessage(FetchMessageRequest req, StreamObserver<FetchMessageResponse> responseObserver) {
        FetchMessageResponse.Builder responseBuilder = FetchMessageResponse.newBuilder();
        int nextOffset = req.getStartingOffset() + 1;
        try {
            // Note: Topic is determined by the broker/partition, not from the request
            // The FetchMessageRequest only contains partition_id and starting_offset
            // TODO: This is a design issue - consumer should specify which topic to consume from
            String defaultTopic = "default"; // Temporary fix
            Message msg = this.broker.consumeMessage(defaultTopic, req.getPartitionId(), req.getStartingOffset());
            if (msg != null) {
                responseBuilder
                        .setMessage(msg)
                        .setStatus(Status.SUCCESS)
                        .setNextOffset(nextOffset);
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
