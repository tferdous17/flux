package grpc.services;

import broker.Broker;
import io.grpc.stub.StreamObserver;
import proto.CreateTopicsRequest;
import proto.CreateTopicsResult;
import proto.CreateTopicsServiceGrpc;
import proto.Status;

import java.io.IOException;

public class CreateTopicsServiceImpl extends CreateTopicsServiceGrpc.CreateTopicsServiceImplBase {
    Broker broker;

    public CreateTopicsServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void createTopics(CreateTopicsRequest req, StreamObserver<CreateTopicsResult> responseObserver) {
        CreateTopicsResult.Builder resultBuilder = CreateTopicsResult.newBuilder();

        try {
            this.broker.createTopics(req.getTopicsList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        resultBuilder.setAcknowledgement("topic creation request received");
        resultBuilder.setStatus(Status.SUCCESS);
        resultBuilder.setTotalNumPartitionsCreated(1);
        resultBuilder.addTopicNames("all topic names");

        responseObserver.onNext(resultBuilder.build()); // this just sends the response back to the client
        responseObserver.onCompleted(); // lets the client know there are no more messages after this
    }

}
