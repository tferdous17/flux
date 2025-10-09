package consumer;

import com.google.protobuf.ByteString;
import consumer.assignors.PartitionAssignor;
import proto.Assignment;

import java.util.*;

/**
 * Leader-only helper that asks an assignor for the full mapping and
 * packs it into a single group-level Assignment blob for SyncGroup.
 * Tracks previous assignment for sticky assignment strategies.
 */
public final class LeaderAssignmentPlanner {
    private final PartitionAssignor assignor;
    private Map<String, Map<String, List<Integer>>> previousAssignment;

    public LeaderAssignmentPlanner(PartitionAssignor assignor) {
        this.assignor = Objects.requireNonNull(assignor, "assignor");
        this.previousAssignment = Collections.emptyMap();
    }

    public Assignment buildGroupAssignment(
            List<String> memberIds,
            Collection<String> topics,
            Map<String, Integer> topicToPartitionCount
    ) {
        Map<String, Map<String, List<Integer>>> full = assignor.assign(
                memberIds,
                topicToPartitionCount,
                previousAssignment
        );

        previousAssignment = deepCopy(full);

        byte[] packed = ProtocolCodec.encodeFullGroupAssignment(full);
        return Assignment.newBuilder()
                .setAssignment(ByteString.copyFrom(packed))
                .build();
    }

    private Map<String, Map<String, List<Integer>>> deepCopy(Map<String, Map<String, List<Integer>>> source) {
        Map<String, Map<String, List<Integer>>> copy = new LinkedHashMap<>();
        for (var entry : source.entrySet()) {
            String memberId = entry.getKey();
            Map<String, List<Integer>> memberTopics = entry.getValue();
            copy.put(memberId, copyTopicPartitions(memberTopics));
        }
        return copy;
    }

    private Map<String, List<Integer>> copyTopicPartitions(Map<String, List<Integer>> topicPartitions) {
        Map<String, List<Integer>> copy = new LinkedHashMap<>();
        for (var entry : topicPartitions.entrySet()) {
            copy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return copy;
    }
}
