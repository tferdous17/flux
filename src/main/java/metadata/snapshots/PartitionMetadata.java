package metadata.snapshots;

import proto.PartitionDetails;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents an immutable snapshot of a partition's metadata
 */
public record PartitionMetadata(int partitionId, String brokerId) {

    public static PartitionMetadata from(proto.PartitionDetails details) {
        return new PartitionMetadata(details.getPartitionId(), details.getBrokerId());
    }

    public static Map<Integer, PartitionMetadata> fromMap(Map<Integer, PartitionDetails> detailsMap) {
        Map<Integer, PartitionMetadata> metadataMap = new HashMap<>();
        detailsMap.forEach((id, details) -> {
            metadataMap.put(id, from(details));
        });
        return metadataMap;
    }

    public static PartitionDetails toDetailsProto(PartitionMetadata metadata) {
        return PartitionDetails
                .newBuilder()
                .setPartitionId(metadata.partitionId)
                .setBrokerId(metadata.brokerId)
                .build();
    }

    public static Map<Integer, PartitionDetails> toDetailsMapProto(Map<Integer, PartitionMetadata> metadataMap) {
        Map<Integer, PartitionDetails> detailsMap = new HashMap<>();
        metadataMap.forEach((id, details) -> {
           detailsMap.put(id, toDetailsProto(details));
        });
        return detailsMap;
    };
}
