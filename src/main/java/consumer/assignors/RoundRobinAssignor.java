package consumer.assignors;

import java.util.*;

public final class RoundRobinAssignor implements PartitionAssignor {
    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        if (memberIds == null || memberIds.isEmpty()) {
            throw new IllegalArgumentException("memberIds must not be empty");
        }
        Map<String, Map<String, List<Integer>>> full = new LinkedHashMap<>();
        for (String mid : memberIds) full.put(mid, new LinkedHashMap<>());

        if (topicToPartitionCount == null || topicToPartitionCount.isEmpty()) return full;

        int m = memberIds.size();
        for (Map.Entry<String, Integer> e : topicToPartitionCount.entrySet()) {
            String topic = e.getKey();
            int partitions = Math.max(0, e.getValue() == null ? 0 : e.getValue());
            for (int p = 0; p < partitions; p++) {
                String owner = memberIds.get(p % m);
                full.get(owner).computeIfAbsent(topic, t -> new ArrayList<>()).add(p);
            }
        }
        return full;
    }
}
