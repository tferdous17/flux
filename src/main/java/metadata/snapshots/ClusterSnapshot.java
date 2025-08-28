package metadata.snapshots;

import java.util.Map;

/**
 * Represents an immutable, and fairly comprehensive snapshot of a cluster's entire metadata
 */
public record ClusterSnapshot(ControllerMetadata controllerMetadata,
                              Map<String, BrokerMetadata> brokers, // <broker id, metadata>
                              Map<String, TopicMetadata> topics, // <topic name, metadata>
                              Map<Integer, PartitionMetadata> partitions) { // <partition id, metadata>
}

