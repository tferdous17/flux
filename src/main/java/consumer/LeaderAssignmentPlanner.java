package consumer;

import com.google.protobuf.ByteString;
import consumer.assignors.PartitionAssignor;
import proto.Assignment;

import java.util.*;

/**
 * Leader-only helper that asks an assignor for the full mapping and
 * packs it into a single group-level Assignment blob for SyncGroup.
 */
public final class LeaderAssignmentPlanner {
    private final PartitionAssignor assignor;

    public LeaderAssignmentPlanner(PartitionAssignor assignor) {
        this.assignor = Objects.requireNonNull(assignor, "assignor");
    }

    public Assignment buildGroupAssignment(
            List<String> memberIds,
            Collection<String> topics,
            Map<String, Integer> topicToPartitionCount
    ) {
        Map<String, Map<String, List<Integer>>> full = assignor.assign(memberIds, topicToPartitionCount);

        byte[] packed = ProtocolCodec.encodeFullGroupAssignment(full);
        return Assignment.newBuilder()
                .setAssignment(ByteString.copyFrom(packed))
                .build();
    }
}
