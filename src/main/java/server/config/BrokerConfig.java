package server.config;

import java.util.Properties;

/**
 * Configuration class for Broker with heartbeat and liveness settings
 */
public class BrokerConfig {
    private final long heartbeatIntervalMs;
    private final int missedHeartbeatThreshold;
    private final long heartbeatTimeoutMs;
    private final long heartbeatRetryBackoffMs;
    private final int maxHeartbeatRetries;
    private final boolean heartbeatEnabled;

    // Default values
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 3000L; // 3 seconds
    private static final int DEFAULT_MISSED_HEARTBEAT_THRESHOLD = 3; // 3 misses before marking offline
    private static final long DEFAULT_HEARTBEAT_TIMEOUT_MS = 5000L; // 5 seconds timeout for heartbeat RPC
    private static final long DEFAULT_HEARTBEAT_RETRY_BACKOFF_MS = 1000L; // 1 second backoff between retries
    private static final int DEFAULT_MAX_HEARTBEAT_RETRIES = 3;
    private static final boolean DEFAULT_HEARTBEAT_ENABLED = true;

    /**
     * Create BrokerConfig with default values
     */
    public BrokerConfig() {
        this.heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;
        this.missedHeartbeatThreshold = DEFAULT_MISSED_HEARTBEAT_THRESHOLD;
        this.heartbeatTimeoutMs = DEFAULT_HEARTBEAT_TIMEOUT_MS;
        this.heartbeatRetryBackoffMs = DEFAULT_HEARTBEAT_RETRY_BACKOFF_MS;
        this.maxHeartbeatRetries = DEFAULT_MAX_HEARTBEAT_RETRIES;
        this.heartbeatEnabled = DEFAULT_HEARTBEAT_ENABLED;
    }

    /**
     * Create BrokerConfig from Properties
     * 
     * @param props Configuration properties
     */
    public BrokerConfig(Properties props) {
        this.heartbeatIntervalMs = Long.parseLong(
                props.getProperty("heartbeat.interval.ms", String.valueOf(DEFAULT_HEARTBEAT_INTERVAL_MS)));
        validateHeartbeatInterval(this.heartbeatIntervalMs);

        this.missedHeartbeatThreshold = Integer.parseInt(
                props.getProperty("missed.heartbeat.threshold", String.valueOf(DEFAULT_MISSED_HEARTBEAT_THRESHOLD)));
        validateMissedHeartbeatThreshold(this.missedHeartbeatThreshold);

        this.heartbeatTimeoutMs = Long.parseLong(
                props.getProperty("heartbeat.timeout.ms", String.valueOf(DEFAULT_HEARTBEAT_TIMEOUT_MS)));
        validateHeartbeatTimeout(this.heartbeatTimeoutMs);

        this.heartbeatRetryBackoffMs = Long.parseLong(
                props.getProperty("heartbeat.retry.backoff.ms", String.valueOf(DEFAULT_HEARTBEAT_RETRY_BACKOFF_MS)));

        this.maxHeartbeatRetries = Integer.parseInt(
                props.getProperty("max.heartbeat.retries", String.valueOf(DEFAULT_MAX_HEARTBEAT_RETRIES)));

        this.heartbeatEnabled = Boolean.parseBoolean(
                props.getProperty("heartbeat.enabled", String.valueOf(DEFAULT_HEARTBEAT_ENABLED)));
    }

    /**
     * Create BrokerConfig with specified values
     */
    public BrokerConfig(long heartbeatIntervalMs, int missedHeartbeatThreshold,
            long heartbeatTimeoutMs, boolean heartbeatEnabled) {
        validateHeartbeatInterval(heartbeatIntervalMs);
        validateMissedHeartbeatThreshold(missedHeartbeatThreshold);
        validateHeartbeatTimeout(heartbeatTimeoutMs);

        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.missedHeartbeatThreshold = missedHeartbeatThreshold;
        this.heartbeatTimeoutMs = heartbeatTimeoutMs;
        this.heartbeatRetryBackoffMs = DEFAULT_HEARTBEAT_RETRY_BACKOFF_MS;
        this.maxHeartbeatRetries = DEFAULT_MAX_HEARTBEAT_RETRIES;
        this.heartbeatEnabled = heartbeatEnabled;
    }

    public long getHeartbeatIntervalMs() {
        return heartbeatIntervalMs;
    }

    public int getMissedHeartbeatThreshold() {
        return missedHeartbeatThreshold;
    }

    public long getHeartbeatTimeoutMs() {
        return heartbeatTimeoutMs;
    }

    public long getHeartbeatRetryBackoffMs() {
        return heartbeatRetryBackoffMs;
    }

    public int getMaxHeartbeatRetries() {
        return maxHeartbeatRetries;
    }

    public boolean isHeartbeatEnabled() {
        return heartbeatEnabled;
    }

    /**
     * Calculate the maximum time before a broker is considered offline
     * 
     * @return Time in milliseconds
     */
    public long getMaxTimeBeforeOfflineMs() {
        return heartbeatIntervalMs * missedHeartbeatThreshold;
    }

    private void validateHeartbeatInterval(long intervalMs) {
        final long MIN_INTERVAL = 100L; // 100ms minimum
        final long MAX_INTERVAL = 60000L; // 60 seconds maximum

        if (intervalMs < MIN_INTERVAL || intervalMs > MAX_INTERVAL) {
            throw new IllegalArgumentException(
                    "Heartbeat interval must be between " + MIN_INTERVAL + "ms and " + MAX_INTERVAL + "ms");
        }
    }

    private void validateMissedHeartbeatThreshold(int threshold) {
        final int MIN_THRESHOLD = 1;
        final int MAX_THRESHOLD = 10;

        if (threshold < MIN_THRESHOLD || threshold > MAX_THRESHOLD) {
            throw new IllegalArgumentException(
                    "Missed heartbeat threshold must be between " + MIN_THRESHOLD + " and " + MAX_THRESHOLD);
        }
    }

    private void validateHeartbeatTimeout(long timeoutMs) {
        final long MIN_TIMEOUT = 100L; // 100ms minimum
        final long MAX_TIMEOUT = 30000L; // 30 seconds maximum

        if (timeoutMs < MIN_TIMEOUT || timeoutMs > MAX_TIMEOUT) {
            throw new IllegalArgumentException(
                    "Heartbeat timeout must be between " + MIN_TIMEOUT + "ms and " + MAX_TIMEOUT + "ms");
        }
    }
}