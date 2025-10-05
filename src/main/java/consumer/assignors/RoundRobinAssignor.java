package consumer.assignors;

import commons.TopicPartition;

import java.util.*;

/**
 * Round-robin assignor
 * 1) Sort memberIds
 * 2) Expand topicToPartitionCount into a flatten sorted list of (topic, partitionId).
 * 3) Deal partitions to members in a cycle
 * 4) Return map: memberId -> (topic -> list(partitionIds)).
 */
public final class RoundRobinAssignor implements PartitionAssignor {
    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        // Edge cases
        if (
            memberIds == null || memberIds.isEmpty() ||
            topicToPartitionCount == null || topicToPartitionCount.isEmpty()
        ) {
            return Collections.emptyMap();
        }

        // Deterministic order of members & topics
        List<String> members = new ArrayList<>(memberIds);
        Collections.sort(members);

        List<String> topics = new ArrayList<>(topicToPartitionCount.keySet());
        Collections.sort(topics);

        // Flatten into (topic, partitionId) list in deterministic order
        List<TopicPartition> all = new ArrayList<>();
        for (String topic : topics) {
            int count = topicToPartitionCount.getOrDefault(topic, 0);
            for (int p = 0; p < count; p++) {
                all.add(new TopicPartition(topic, p));
            }
        }

        // If no partitions at all, return empty assignments for each member
        if (all.isEmpty()) {
            Map<String, Map<String, List<Integer>>> empty = new LinkedHashMap<>();
            for (String m : members) {
                empty.put(m, Collections.emptyMap());
            }
            return empty;
        }

        // Prepare result structure: member -> topic -> partitions
        Map<String, Map<String, List<Integer>>> result = new LinkedHashMap<>();
        for (String m : members) {
            result.put(m, new LinkedHashMap<>());
        }

        // Deal out in round-robin
        int i = 0;
        for (TopicPartition tp : all) {
            String member = members.get(i % members.size());
            Map<String, List<Integer>> byTopic = result.get(member);
            byTopic.computeIfAbsent(tp.getTopic(), t -> new ArrayList<>()).add(tp.getPartition());
            i++;
        }

        //  Wrap per-topic lists as unmodifiable for safety
        for (Map<String, List<Integer>> byTopic : result.values()) {
            for (Map.Entry<String, List<Integer>> e : byTopic.entrySet()) {
                e.setValue(Collections.unmodifiableList(e.getValue()));
            }
        }
        return result;
    }
}
