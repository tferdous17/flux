package metadata.snapshots;

/**
 * Represents an immutable snapshot of the broker metadata
 */
public record BrokerMetadata(String brokerId,
                             String host,
                             int port,
                             int numPartitions) {
}
