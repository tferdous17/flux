package grpc.services;

import metadata.snapshots.BrokerMetadata;
import metadata.snapshots.PartitionMetadata;
import proto.*;
import server.internal.Broker;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.Map;

// This service is found on top of the Controller node
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

    @Override
    public void fetchClusterMetadata(FetchClusterMetadataRequest request, StreamObserver<FetchClusterMetadataResponse> responseObserver) {
        if (!broker.isActiveController()) {
            return;
        }

        FetchClusterMetadataResponse.Builder response = FetchClusterMetadataResponse.newBuilder();

        // Build ControllerDetails
        proto.ControllerDetails controllerDetails = proto.ControllerDetails
                .newBuilder()
                .setControllerId(broker.getBrokerId())
                .addAllFollowerNodeEndpoints(broker.getFollowerNodeEndpoints().values())
                .setIsActive(true)
                .build();
        response.setControllerDetails(controllerDetails);

        // We must also build out the Controller's broker metadata as well
        response.putBrokerDetails(
                "%s:%d".formatted(broker.getHost(), broker.getPort()),
                proto.BrokerDetails
                        .newBuilder()
                        .setBrokerId(this.broker.getBrokerId())
                        .setHost(this.broker.getHost())
                        .setPort(this.broker.getPort())
                        .setNumPartitions(this.broker.getNumPartitions())
                        .putAllPartitionDetails(PartitionMetadata
                                .toDetailsMapProto(this.broker
                                        .getControllerMetadata()
                                        .get()
                                        .partitionMetadata()
                                )
                        )
                        .build()
        );

        // For each broker in the cluster, create a BrokerDetails object for it and put it in the map w/ its broker addr as key
        for (Map.Entry<String, BrokerMetadata> entry : broker.getCachedFollowerMetadata().entrySet()) {
            proto.BrokerDetails details = proto.BrokerDetails
                    .newBuilder()
                    .setBrokerId(entry.getValue().brokerId())
                    .setHost(entry.getValue().host())
                    .setPort(entry.getValue().port())
                    .setNumPartitions(entry.getValue().numPartitions())
                    .putAllPartitionDetails(PartitionMetadata.toDetailsMapProto(entry.getValue().partitionMetadata()))
                    .build();

            response.putBrokerDetails(entry.getKey(), details);
        }

        // TODO: TopicDetails should be here, but the way topic metadata is currently managed will probs get reworked soon (and i got lost in the sauce).
        //  Implement TopicDetails into the response when that's done
        response.putAllTopicDetails(new HashMap<>());

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

}
