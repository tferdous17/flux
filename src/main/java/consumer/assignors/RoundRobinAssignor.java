package consumer.assignors;

import java.util.*;

// TODO: Investigate RoundRobinAssignor
public final class RoundRobinAssignor implements PartitionAssignor {
    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        return null;
    }
}
