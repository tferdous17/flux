package grpc.services;

import broker.Broker;
import io.grpc.stub.StreamObserver;
import proto.FetchBrokerMetadataRequest;
import proto.FetchBrokerMetadataResponse;
import proto.MetadataServiceGrpc;

public class MetadataServiceImpl extends MetadataServiceGrpc.MetadataServiceImplBase {
    private Broker broker;

    public MetadataServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void fetchBrokerMetadata(FetchBrokerMetadataRequest request, StreamObserver<FetchBrokerMetadataResponse> responseObserver) {
        FetchBrokerMetadataResponse response = FetchBrokerMetadataResponse
                .newBuilder()
                .setBrokerId(broker.getBrokerId())
                .setHost(broker.getHost())
                .setPortNumber(broker.getPort())
                .setNumPartitions(broker.getNumPartitions())
                .build();

        responseObserver.onNext(response); // this just sends the response back to the client
        responseObserver.onCompleted(); // lets the client know there are no more messages after this
    }
}
