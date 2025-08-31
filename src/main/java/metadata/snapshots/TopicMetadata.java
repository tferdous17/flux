package metadata.snapshots;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents an immutable snapshot of a topic's metadata
 */
public record TopicMetadata(String topicName,
                            int numPartitions,
                            Map<Integer, PartitionMetadata> partitions) {

    public static TopicMetadata from(proto.TopicDetails details) {
        return new TopicMetadata(
                details.getTopicName(),
                details.getNumPartitions(),
                PartitionMetadata.fromMap(details.getPartitionDetailsMap())
        );
    }

    public static Map<String, TopicMetadata> fromMap(Map<String, proto.TopicDetails> detailsMap) {
        Map<String, TopicMetadata> metadataMap = new HashMap<>();
        detailsMap.forEach((topicName, details) -> {
            metadataMap.put(topicName, from(details));
        });
        return metadataMap;
    }
}
