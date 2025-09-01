package metadata.snapshots;

import java.util.Map;

/**
 * Represents an immutable, and fairly comprehensive snapshot of a cluster's entire metadata
 */
public record ClusterSnapshot(ControllerMetadata controllerMetadata,
                              Map<String, BrokerMetadata> brokers, // <broker addr, metadata>
                              Map<String, TopicMetadata> topics) { // <topic name, metadata>
}

