package grpc.services;

import io.grpc.stub.StreamObserver;
import metadata.snapshots.BrokerMetadata;
import metadata.snapshots.PartitionMetadata;
import org.tinylog.Logger;
import proto.*;
import server.internal.Broker;

import java.util.HashMap;

/**
 * Below methods are handled by the Controller node specifically.
 * Non-controller nodes are not expected to return responses of any kind, but do send requests (i.e., BrokerRegistrationRequest)
 */
public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {
    Broker broker;

    public ControllerServiceImpl(Broker broker) {
        this.broker  = broker;
    }

    // Each broker will send an UpdateBrokerMetadata request to the controller, giving it its most up-to-date metadata.
    @Override
    public void updateBrokerMetadata(UpdateBrokerMetadataRequest req, StreamObserver<UpdateBrokerMetadataResponse> responseObserver) {
        if (!broker.isActiveController()) {
            return;
        }

        UpdateBrokerMetadataResponse.Builder response = UpdateBrokerMetadataResponse
                .newBuilder()
                .setStatus(Status.SUCCESS);

        // only update metadata for a broker if num partitions change (assuming we won't modify the other fields in this system)
        if (broker.getCachedFollowerMetadata().containsKey(req.getBrokerId())
                && broker.getCachedFollowerMetadata().get(req.getBrokerId()).numPartitions() == req.getNumPartitions()) {
            response.setAcknowledgement("Cached metadata for broker=%s already equal to updated metadata. Cache refresh not needed.".formatted(req.getBrokerId()));
        } else {
            response.setAcknowledgement("ACK: Received updated broker metadata from broker=%s. Updating cache".formatted(req.getBrokerId()));
            broker.getCachedFollowerMetadata().put(
                    req.getBrokerId(),
                    new BrokerMetadata(
                            req.getBrokerId(),
                            req.getHost(),
                            req.getPortNumber(),
                            req.getNumPartitions(),
                            PartitionMetadata.fromMap(req.getPartitionDetailsMap())
                    )
            );
        }

        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void registerBroker(BrokerRegistrationRequest req, StreamObserver<BrokerRegistrationResult> responseObserver) {
        if (!broker.isActiveController()) {
            return;
        }

        BrokerRegistrationResult response = BrokerRegistrationResult
                .newBuilder()
                .setAcknowledgement("ACK: Controller {broker=%s, address=%s:%d} received BrokerRegistrationRequest from {node=%s, address=%s:%d}".formatted(
                        this.broker.getBrokerId(),
                        this.broker.getHost(),
                        this.broker.getPort(),
                        req.getBrokerId(),
                        req.getBrokerHost(),
                        req.getBrokerPort()
                        )
                )
                .setStatus(Status.SUCCESS)
                .build();

        broker.getFollowerNodeEndpoints()
                .put(req.getBrokerId(), "%s:%d".formatted(req.getBrokerHost(), req.getBrokerPort()));

        // Newly created & registered brokers will have 0 partitions by default
        broker.getCachedFollowerMetadata().put(
                req.getBrokerId(),
                new BrokerMetadata(
                        req.getBrokerId(),
                        req.getBrokerHost(),
                        req.getBrokerPort(),
                        0,
                        new HashMap<>()
                )
        );

        Logger.info("Current follower nodes: " + broker.getFollowerNodeEndpoints().toString());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void decommissionBroker(DecommissionBrokerRequest req, StreamObserver<DecommissionBrokerResult> responseObserver) {
        if (!broker.isActiveController()) {
            return;
        }

        DecommissionBrokerResult response = DecommissionBrokerResult.newBuilder()
                .setAcknowledgement("ACK: Controller {broker=%s, address=%s:%d} received DecommissionBrokerRequest from {node=%s}. Removing this broker from followers and gracefully shutting it down."
                        .formatted(
                                this.broker.getBrokerId(),
                                this.broker.getHost(),
                                this.broker.getPort(),
                                req.getBrokerId()
                        )
                )
                .setStatus(Status.BROKER_SHUTDOWN_IN_PROGRESS)
                .build();

        broker.getFollowerNodeEndpoints().remove(req.getBrokerId());

        Logger.info("Removed follower node {node=%s} from this cluster with Controller {broker=%s, address=%s:%d}. Broker is clear to gracefully shutdown."
                .formatted(
                        req.getBrokerId(),
                        this.broker.getBrokerId(),
                        this.broker.getHost(),
                        this.broker.getPort()
                )
        );

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
