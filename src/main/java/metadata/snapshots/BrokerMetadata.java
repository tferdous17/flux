package metadata.snapshots;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents an immutable snapshot of a broker's metadata
 */
public record BrokerMetadata(String brokerId,
                             String host,
                             int port,
                             int numPartitions,
                             Map<Integer, PartitionMetadata> partitionMetadata) {

    public static BrokerMetadata from(proto.BrokerDetails details) {
        return new BrokerMetadata(
                details.getBrokerId(),
                details.getHost(),
                details.getPort(),
                details.getNumPartitions(),
                PartitionMetadata.fromMap(details.getPartitionDetailsMap())
        );
    }

    public static Map<String, BrokerMetadata> fromMap(Map<String, proto.BrokerDetails> detailsMap) {
        Map<String, BrokerMetadata> metadataMap = new HashMap<>();
        detailsMap.forEach((id, details) -> {
            metadataMap.put(id, from(details));
        });
        return metadataMap;
    }
}
