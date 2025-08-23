package grpc.services;

import io.grpc.stub.StreamObserver;
import org.tinylog.Logger;
import proto.*;
import server.internal.Broker;

/**
 * Below methods are handled by the Controller node specifically.
 * Non-controller nodes are not expected to return responses of any kind, but do send requests (i.e., BrokerRegistrationRequest)
 */
public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {
    Broker broker;

    public ControllerServiceImpl(Broker broker) {
        this.broker  = broker;
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

        Logger.info("Current follower nodes: " + broker.getFollowerNodeEndpoints().toString());

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void unregisterBroker(UnregisterBrokerRequest req, StreamObserver<UnregisterBrokerResult> responseObserver) {
        if (!broker.isActiveController()) {
            return;
        }

        UnregisterBrokerResult response = UnregisterBrokerResult.newBuilder()
                .setAcknowledgement("ACK: Controller {broker=%s, address=%s:%d} received UnregisterBrokerRequest from {node=%s}. Removing broker from followers."
                        .formatted(
                                this.broker.getBrokerId(),
                                this.broker.getHost(),
                                this.broker.getPort(),
                                req.getBrokerId()
                        )
                )
                .setStatus(Status.SUCCESS)
                .build();

        broker.getFollowerNodeEndpoints().remove(req.getBrokerId());

        Logger.info("Removed follower node {node=%s} from this cluster with Controller {broker=%s, address=%s:%d}"
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
