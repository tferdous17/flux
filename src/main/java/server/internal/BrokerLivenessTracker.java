package server.internal;

import commons.FluxExecutor;
import grpc.BrokerLoadInfo;
import org.tinylog.Logger;
import server.config.BrokerConfig;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Tracks broker liveness through heartbeat timestamps.
 * Used by the controller to monitor broker health.
 */
public class BrokerLivenessTracker {
    private final Map<String, BrokerHeartbeatInfo> brokerHeartbeats;
    private final Map<String, BrokerLoadInfo> brokerLoads;
    private final BrokerConfig config;
    private final AtomicBoolean isRunning;
    private ScheduledFuture<?> monitoringTask;

    /**
     * Internal class to track heartbeat information per broker
     */
    private static class BrokerHeartbeatInfo {
        private volatile long lastHeartbeatTimestamp;
        private volatile long sequenceNumber;
        private volatile boolean isAlive;

        BrokerHeartbeatInfo(long timestamp, long sequenceNumber) {
            this.lastHeartbeatTimestamp = timestamp;
            this.sequenceNumber = sequenceNumber;
            this.isAlive = true;
        }

        void updateHeartbeat(long timestamp, long sequenceNumber) {
            this.lastHeartbeatTimestamp = timestamp;
            this.sequenceNumber = sequenceNumber;
            this.isAlive = true;
        }

        boolean isTimedOut(long currentTime, long timeoutMs) {
            return (currentTime - lastHeartbeatTimestamp) > timeoutMs;
        }
    }

    public BrokerLivenessTracker(BrokerConfig config) {
        this.brokerHeartbeats = new ConcurrentHashMap<>();
        this.brokerLoads = new ConcurrentHashMap<>();
        this.config = config != null ? config : new BrokerConfig();
        this.isRunning = new AtomicBoolean(false);
    }

    /**
     * Start monitoring for timed-out heartbeats
     */
    public void startMonitoring() {
        if (isRunning.compareAndSet(false, true)) {
            long checkInterval = config.getHeartbeatIntervalMs();
            monitoringTask = FluxExecutor.getSchedulerService().scheduleWithFixedDelay(
                    this::checkForTimedOutBrokers,
                    checkInterval,
                    checkInterval,
                    TimeUnit.MILLISECONDS);
            Logger.info("Started broker liveness monitoring with interval {}ms", checkInterval);
        }
    }

    /**
     * Stop monitoring
     */
    public void stopMonitoring() {
        if (isRunning.compareAndSet(true, false)) {
            if (monitoringTask != null) {
                monitoringTask.cancel(false);
                monitoringTask = null;
            }
            Logger.info("Stopped broker liveness monitoring");
        }
    }

    /**
     * Record a heartbeat from a broker
     */
    public void recordHeartbeat(String brokerId, long timestamp, long sequenceNumber) {
        BrokerHeartbeatInfo info = brokerHeartbeats.compute(brokerId, (id, existing) -> {
            if (existing == null) {
                Logger.info("Broker {} registered with liveness tracker", brokerId);
                return new BrokerHeartbeatInfo(timestamp, sequenceNumber);
            } else {
                existing.updateHeartbeat(timestamp, sequenceNumber);
                if (!existing.isAlive) {
                    Logger.info("Broker {} marked as alive after heartbeat", brokerId);
                }
                return existing;
            }
        });
    }

    /**
     * Check if a broker is currently alive
     */
    public boolean isBrokerAlive(String brokerId) {
        BrokerHeartbeatInfo info = brokerHeartbeats.get(brokerId);
        return info != null && info.isAlive;
    }

    /**
     * Get all currently alive broker IDs
     */
    public Set<String> getAliveBrokers() {
        return brokerHeartbeats.entrySet().stream()
                .filter(entry -> entry.getValue().isAlive)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Get the last heartbeat timestamp for a broker
     */
    public long getLastHeartbeatTimestamp(String brokerId) {
        BrokerHeartbeatInfo info = brokerHeartbeats.get(brokerId);
        return info != null ? info.lastHeartbeatTimestamp : -1;
    }

    /**
     * Manually mark a broker as offline
     */
    public void markBrokerOffline(String brokerId) {
        BrokerHeartbeatInfo info = brokerHeartbeats.get(brokerId);
        if (info != null && info.isAlive) {
            info.isAlive = false;
            Logger.warn("Broker {} manually marked as offline", brokerId);
        }
    }

    /**
     * Check for brokers that have timed out
     */
    private void checkForTimedOutBrokers() {
        long currentTime = System.currentTimeMillis();
        long timeoutMs = config.getHeartbeatIntervalMs() * config.getMissedHeartbeatThreshold();

        brokerHeartbeats.forEach((brokerId, info) -> {
            if (info.isAlive && info.isTimedOut(currentTime, timeoutMs)) {
                info.isAlive = false;
                Logger.warn("Broker {} marked as offline - no heartbeat for {}ms",
                        brokerId, (currentTime - info.lastHeartbeatTimestamp));
            }
        });
    }

    /**
     * Get count of alive brokers
     */
    public int getAliveBrokerCount() {
        return (int) brokerHeartbeats.values().stream()
                .filter(info -> info.isAlive)
                .count();
    }

    /**
     * Remove a broker from tracking
     */
    public void removeBroker(String brokerId) {
        BrokerHeartbeatInfo removed = brokerHeartbeats.remove(brokerId);
        brokerLoads.remove(brokerId);
        if (removed != null) {
            Logger.info("Broker {} removed from liveness tracker", brokerId);
        }
    }

    /**
     * Record broker load information
     */
    public void recordLoadInfo(String brokerId, BrokerLoadInfo loadInfo) {
        if (loadInfo != null) {
            brokerLoads.put(brokerId, loadInfo);
            Logger.trace("Updated load info for broker {}: partitions={}, connections={}, memory={}MB, cpu={}%",
                    brokerId,
                    loadInfo.getPartitionCount(),
                    loadInfo.getConnectionCount(),
                    loadInfo.getAvailableMemory() / (1024 * 1024),
                    loadInfo.getCpuUsage());
        }
    }

    /**
     * Get load information for a specific broker
     */
    public BrokerLoadInfo getBrokerLoadInfo(String brokerId) {
        return brokerLoads.get(brokerId);
    }

    /**
     * Get the broker ID with the least load (for future partition placement)
     * Currently uses partition count as the primary metric
     */
    public String getLeastLoadedBroker() {
        String leastLoadedBroker = null;
        int minPartitions = Integer.MAX_VALUE;

        for (Map.Entry<String, BrokerLoadInfo> entry : brokerLoads.entrySet()) {
            String brokerId = entry.getKey();
            BrokerLoadInfo loadInfo = entry.getValue();

            // Only consider alive brokers
            if (isBrokerAlive(brokerId) && loadInfo.getPartitionCount() < minPartitions) {
                minPartitions = loadInfo.getPartitionCount();
                leastLoadedBroker = brokerId;
            }
        }

        return leastLoadedBroker;
    }

    /**
     * Get all broker load information for alive brokers
     */
    public Map<String, BrokerLoadInfo> getAllBrokerLoads() {
        return brokerLoads.entrySet().stream()
                .filter(entry -> isBrokerAlive(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}