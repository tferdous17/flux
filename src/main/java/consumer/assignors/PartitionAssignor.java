package consumer.assignors;

import java.util.List;
import java.util.Map;


public interface PartitionAssignor {
    Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    );
}
