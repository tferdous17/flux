package consumer.assignors;

import consumer.assignors.PartitionAssignor;

import java.util.*;

// TODO: Investigate RangeAssignor
public final class RangeAssignor implements PartitionAssignor {

    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        return null;
    }
}
