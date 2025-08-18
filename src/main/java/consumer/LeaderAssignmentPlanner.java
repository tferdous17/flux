import com.google.protobuf.ByteString;
import proto.Assignment;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface PartitionAssignor {
    Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    );
}

public final class RangeAssignor implements PartitionAssignor {
    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> tpc
    ) { /* range logic here */ }
}

public final class LeaderAssignmentPlanner {
    private final PartitionAssignor assignor;

    public LeaderAssignmentPlanner(PartitionAssignor assignor) {
        this.assignor = assignor;
    }

    // Produces the group-level blob your server wants in SyncGroup
    public Assignment buildGroupAssignment(
            List<String> memberIds,
            Collection<String> topics,
            Map<String, Integer> topicToPartitionCount
    ) {
        var full = assignor.assign(memberIds, topicToPartitionCount);
        byte[] packed = encodeFullGroupAssignment(full); // your serializer
        return Assignment.newBuilder()
                .setAssignment(ByteString.copyFrom(packed))
                .build();
    }
}
