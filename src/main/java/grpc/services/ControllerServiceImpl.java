package grpc.services;

import io.grpc.stub.StreamObserver;
import org.tinylog.Logger;
import proto.BrokerRegistrationRequest;
import proto.BrokerRegistrationResult;
import proto.ControllerServiceGrpc;
import proto.Status;
import server.internal.Broker;

public class ControllerServiceImpl extends ControllerServiceGrpc.ControllerServiceImplBase {
    Broker broker;

    public ControllerServiceImpl(Broker broker) {
        this.broker  = broker;
    }

    @Override
    public void registerBroker(BrokerRegistrationRequest req, StreamObserver<BrokerRegistrationResult> responseObserver) {
        // Non-controller nodes should not be returning a response for Broker registration.
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
}
