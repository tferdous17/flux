package consumer.assignors;

import java.util.*;

/**
 * RangeAssignor:
 * For each topic independently:
 *  - Sort members and assign each member one contiguous "range" of partitions.
 *  - If partitions don't divide evenly, the first (count % numMembers) members get one extra.
 * Output shape: memberId -> (topic -> List(partitionIds)).
 */
public final class RangeAssignor implements PartitionAssignor {

    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {

        if (
                memberIds == null || memberIds.isEmpty() ||
                topicToPartitionCount == null || topicToPartitionCount.isEmpty()
        ) {
            return Collections.emptyMap();
        }

        List<String> members = new ArrayList<>(memberIds);
        List<String> topics = new ArrayList<>(topicToPartitionCount.keySet());

        Collections.sort(members);
        Collections.sort(topics);

        // member -> topic -> partitions
        Map<String, Map<String, List<Integer>>> result = new LinkedHashMap<>();
        for (String m : members) {
            result.put(m, new LinkedHashMap<>());
        }

        // Assign ranges per topic
        for (String topic : topics) {
            int partitionCount = Math.max(0, topicToPartitionCount.getOrDefault(topic, 0));
            if (partitionCount == 0) {
                // still create empty lists for determinism if desired; safe to skip as well
                continue;
            }

            int numMembers = members.size();
            int base = partitionCount / numMembers; // MIN PER MEMBER
            int extra = partitionCount % numMembers; // first EXTRA members get an extra one.

            int nextStart = 0;
            for (int i = 0; i < numMembers; i++) {
                int len = base + (i < extra ? 1 : 0);

                // if size is zero, this member gets none for this topic
                if (len > 0) {
                    List<Integer> range = new ArrayList<>(len);
                    for (int p = nextStart; p < nextStart + len; p++) {
                        range.add(p);
                    }
                    result.get(members.get(i))
                            .computeIfAbsent(topic, t -> new ArrayList<>())
                            .addAll(range);
                    nextStart += len;
                }
            }
        }

        // freeze the lists
        for (Map<String, List<Integer>> byTopic : result.values()) {
            for (Map.Entry<String, List<Integer>> e : byTopic.entrySet()) {
                e.setValue(Collections.unmodifiableList(e.getValue()));
            }
        }

        return result;
    }
}
