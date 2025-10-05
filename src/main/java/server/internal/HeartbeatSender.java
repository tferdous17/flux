package server.internal;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import commons.FluxExecutor;
import grpc.BrokerLoadInfo;
import grpc.HeartbeatRequest;
import grpc.HeartbeatResponse;
import grpc.HeartbeatServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.tinylog.Logger;
import server.config.BrokerConfig;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages heartbeat sending from broker to controller
 */
public class HeartbeatSender {
    private final String brokerId;
    private final String brokerHost;
    private final int brokerPort;
    private final BrokerConfig config;
    private final AtomicLong sequenceNumber = new AtomicLong(0);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private ManagedChannel channel;
    private HeartbeatServiceGrpc.HeartbeatServiceFutureStub heartbeatStub;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> heartbeatTask;

    // Metrics
    private final AtomicLong lastHeartbeatTime = new AtomicLong(0);
    private final AtomicLong failedHeartbeats = new AtomicLong(0);
    private final AtomicLong successfulHeartbeats = new AtomicLong(0);

    public HeartbeatSender(String brokerId, String brokerHost, int brokerPort, BrokerConfig config) {
        this.brokerId = brokerId;
        this.brokerHost = brokerHost;
        this.brokerPort = brokerPort;
        this.config = config;
        this.scheduler = FluxExecutor.getSchedulerService();
    }

    /**
     * Start sending heartbeats to the controller
     * 
     * @param controllerEndpoint The controller endpoint (e.g., "localhost:50051")
     */
    public void start(String controllerEndpoint) {
        if (!config.isHeartbeatEnabled()) {
            Logger.info("Heartbeat is disabled for broker {}", brokerId);
            return;
        }

        if (isRunning.compareAndSet(false, true)) {
            Logger.info("Starting heartbeat sender for broker {} to controller {}",
                    brokerId, controllerEndpoint);

            // Create channel and stub
            channel = ManagedChannelBuilder.forTarget(controllerEndpoint)
                    .usePlaintext()
                    .build();
            heartbeatStub = HeartbeatServiceGrpc.newFutureStub(channel);

            // Schedule periodic heartbeats
            heartbeatTask = scheduler.scheduleWithFixedDelay(
                    this::sendHeartbeat,
                    0, // Initial delay
                    config.getHeartbeatIntervalMs(),
                    TimeUnit.MILLISECONDS);

            Logger.info("Heartbeat sender started for broker {}", brokerId);
        } else {
            Logger.warn("Heartbeat sender already running for broker {}", brokerId);
        }
    }

    /**
     * Stop sending heartbeats
     */
    public void stop() {
        if (isRunning.compareAndSet(true, false)) {
            Logger.info("Stopping heartbeat sender for broker {}", brokerId);

            // Cancel scheduled task
            if (heartbeatTask != null) {
                heartbeatTask.cancel(false);
                heartbeatTask = null;
            }

            // Shutdown channel
            if (channel != null) {
                channel.shutdown();
                try {
                    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                        channel.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    channel.shutdownNow();
                    Thread.currentThread().interrupt();
                }
                channel = null;
            }

            Logger.info("Heartbeat sender stopped for broker {}", brokerId);
        }
    }

    /**
     * Send a single heartbeat to the controller
     */
    private void sendHeartbeat() {
        if (!isRunning.get()) {
            return;
        }

        try {
            // Build heartbeat request
            HeartbeatRequest request = HeartbeatRequest.newBuilder()
                    .setBrokerId(Integer.parseInt(brokerId.replaceAll("[^0-9]", "")))
                    .setHost(brokerHost)
                    .setPort(brokerPort)
                    .setTimestamp(System.currentTimeMillis())
                    .setSequenceNumber(sequenceNumber.incrementAndGet())
                    .setLoadInfo(buildLoadInfo())
                    .build();

            // Send heartbeat asynchronously
            ListenableFuture<HeartbeatResponse> future = heartbeatStub
                    .withDeadlineAfter(config.getHeartbeatTimeoutMs(), TimeUnit.MILLISECONDS)
                    .sendHeartbeat(request);

            Futures.addCallback(future, new FutureCallback<HeartbeatResponse>() {
                @Override
                public void onSuccess(HeartbeatResponse response) {
                    handleHeartbeatResponse(response);
                }

                @Override
                public void onFailure(Throwable t) {
                    handleHeartbeatFailure(t);
                }
            }, FluxExecutor.getExecutorService());

        } catch (Exception e) {
            Logger.error("Error sending heartbeat from broker {}: {}", brokerId, e.getMessage());
            failedHeartbeats.incrementAndGet();
        }
    }

    /**
     * Build load information for the heartbeat
     */
    private BrokerLoadInfo buildLoadInfo() {
        Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long availableMemory = maxMemory - (totalMemory - freeMemory);

        // Simple CPU usage approximation (not accurate, for demonstration)
        double cpuUsage = Math.min(100.0, Math.random() * 30 + 10); // Placeholder

        return BrokerLoadInfo.newBuilder()
                .setPartitionCount(0) // Will be updated when integrated with Broker
                .setConnectionCount(0) // Will be updated when connection tracking is added
                .setAvailableMemory(availableMemory)
                .setCpuUsage(cpuUsage)
                .build();
    }

    /**
     * Handle successful heartbeat response
     */
    private void handleHeartbeatResponse(HeartbeatResponse response) {
        lastHeartbeatTime.set(System.currentTimeMillis());
        successfulHeartbeats.incrementAndGet();

        if (!response.getAccepted()) {
            Logger.warn("Heartbeat not accepted by controller: {}", response.getErrorMessage());
        }

        // Handle controller directives if any
        if (response.hasDirective()) {
            processControllerDirective(response.getDirective());
        }

        Logger.debug("Heartbeat {} successful for broker {}", sequenceNumber.get(), brokerId);
    }

    /**
     * Handle heartbeat failure
     */
    private void handleHeartbeatFailure(Throwable t) {
        failedHeartbeats.incrementAndGet();
        Logger.error("Heartbeat failed for broker {}: {}", brokerId, t.getMessage());

        // Implement retry logic if needed
        long consecutiveFailures = failedHeartbeats.get() - successfulHeartbeats.get();
        if (consecutiveFailures >= config.getMaxHeartbeatRetries()) {
            Logger.error("Max heartbeat retries exceeded for broker {}", brokerId);
            // Could trigger reconnection or other recovery actions here
        }
    }

    /**
     * Process directives from controller
     */
    private void processControllerDirective(grpc.ControllerDirective directive) {
        Logger.info("Received directive {} from controller for broker {}: {}",
                directive.getType(), brokerId, directive.getDetails());

        switch (directive.getType()) {
            case SHUTDOWN:
                Logger.info("Shutdown directive received for broker {}", brokerId);
                // Trigger graceful shutdown
                break;
            case REBALANCE:
                Logger.info("Rebalance directive received for broker {}", brokerId);
                // Trigger partition rebalancing
                break;
            case UPDATE_CONFIG:
                Logger.info("Update config directive received for broker {}", brokerId);
                // Update broker configuration
                break;
            case NONE:
            default:
                // No action needed
                break;
        }
    }

    /**
     * Get the time of last successful heartbeat
     */
    public long getLastHeartbeatTime() {
        return lastHeartbeatTime.get();
    }

    /**
     * Get the number of failed heartbeats
     */
    public long getFailedHeartbeats() {
        return failedHeartbeats.get();
    }

    /**
     * Get the number of successful heartbeats
     */
    public long getSuccessfulHeartbeats() {
        return successfulHeartbeats.get();
    }

    /**
     * Check if heartbeat sender is running
     */
    public boolean isRunning() {
        return isRunning.get();
    }
}