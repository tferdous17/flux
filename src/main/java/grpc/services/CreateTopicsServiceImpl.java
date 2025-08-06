package grpc.services;

import broker.Broker;
import io.grpc.stub.StreamObserver;
import proto.*;

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

        resultBuilder.setAcknowledgement("Topic Creation request received");
        resultBuilder.setStatus(Status.SUCCESS);
        resultBuilder.addTopicNames(
                req.getTopicsList()
                        .stream()
                        .map(Topic::getTopicName)
                        .toList()
                        .toString()
        );
        resultBuilder.setTotalNumPartitionsCreated(
                req.getTopicsList()
                        .stream()
                        .map(t -> t.getNumPartitions())
                        .reduce(0, Integer::sum)
        );

        responseObserver.onNext(resultBuilder.build()); // this just sends the response back to the client
        responseObserver.onCompleted(); // lets the client know there are no more messages after this
    }

}
