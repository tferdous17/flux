package metadata.snapshots;

import java.util.Map;

public record TopicMetadata(String topicName,
                            int numPartitions,
                            Map<Integer, PartitionMetadata> partitions) {
}
