package grpc.services;

import grpc.ControllerDirective;
import grpc.HeartbeatRequest;
import grpc.HeartbeatResponse;
import grpc.HeartbeatServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.tinylog.Logger;
import server.internal.Broker;

/**
 * gRPC service implementation for handling heartbeat requests from brokers
 */
public class HeartbeatServiceImpl extends HeartbeatServiceGrpc.HeartbeatServiceImplBase {
    private final Broker broker;

    public HeartbeatServiceImpl(Broker broker) {
        this.broker = broker;
    }

    @Override
    public void sendHeartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        // Only process heartbeats if this broker is the active controller
        if (!broker.isActiveController()) {
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setAccepted(false)
                    .setErrorMessage("This broker is not the active controller")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        // Validate the request
        if (request.getBrokerId() == 0 || request.getHost().isEmpty()) {
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setAccepted(false)
                    .setErrorMessage("Invalid heartbeat request: missing broker ID or host")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            return;
        }

        String brokerId = String.valueOf(request.getBrokerId());

        // Process the heartbeat through the Controller interface
        broker.processBrokerHeartbeat(brokerId, request);

        Logger.debug("Heartbeat received from broker {} (seq: {}, timestamp: {})",
                brokerId, request.getSequenceNumber(), request.getTimestamp());

        // Build the response
        HeartbeatResponse.Builder responseBuilder = HeartbeatResponse.newBuilder()
                .setAccepted(true)
                .setControllerTimestamp(System.currentTimeMillis());

        // Add any controller directives if needed
        ControllerDirective directive = determineDirective(brokerId);
        if (directive != null) {
            responseBuilder.setDirective(directive);
        }

        HeartbeatResponse response = responseBuilder.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    /**
     * Determine if any directives should be sent to the broker
     */
    private ControllerDirective determineDirective(String brokerId) {
        // For now, no directives are sent
        // TODO: Check for rebalancing needs, configuration updates, etc.
        return null;
    }

    @Override
    public StreamObserver<HeartbeatRequest> sendBatchHeartbeat(
            StreamObserver<HeartbeatResponse> responseObserver) {
        // Batch heartbeat not implemented yet
        return new StreamObserver<HeartbeatRequest>() {
            @Override
            public void onNext(HeartbeatRequest request) {
                HeartbeatResponse response = HeartbeatResponse.newBuilder()
                        .setAccepted(false)
                        .setErrorMessage("Batch heartbeat not implemented")
                        .build();
                responseObserver.onNext(response);
            }

            @Override
            public void onError(Throwable t) {
                Logger.error("Error in batch heartbeat", t);
                responseObserver.onError(t);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
            }
        };
    }
}