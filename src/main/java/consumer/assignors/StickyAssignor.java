package consumer.assignors;

import java.util.*;


// TODO: Investigate StickyAssignor
public class StickyAssignor implements PartitionAssignor {
    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        return null;
    }
}