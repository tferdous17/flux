package consumer.assignors;

import commons.TopicPartition;

import java.util.*;

/**
 * StickyAssignor with heap-based balanced assignment.
 *
 * Guarantees balanced distribution where each member gets ⌊P/M⌋ or ⌈P/M⌉ partitions.
 * Uses greedy heap-based algorithm to assign partitions to least-loaded members.
 *
 * Note: This is a simplified implementation without true "stickiness" since the interface
 * doesn't provide previous assignment information. It focuses on balanced distribution.
 */
public class StickyAssignor implements PartitionAssignor {

    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        if (memberIds == null || memberIds.isEmpty() ||
            topicToPartitionCount == null || topicToPartitionCount.isEmpty()) {
            return Collections.emptyMap();
        }

        List<String> members = new ArrayList<>(memberIds);
        List<String> topics = new ArrayList<>(topicToPartitionCount.keySet());
        Collections.sort(members);
        Collections.sort(topics);

        List<TopicPartition> allPartitions = buildPartitionUniverse(topics, topicToPartitionCount);

        if (allPartitions.isEmpty()) {
            Map<String, Map<String, List<Integer>>> empty = new LinkedHashMap<>();
            for (String m : members) {
                empty.put(m, Collections.emptyMap());
            }
            return empty;
        }

        Map<String, Map<String, List<Integer>>> result = new LinkedHashMap<>();
        for (String m : members) {
            result.put(m, new LinkedHashMap<>());
        }

        int totalPartitions = allPartitions.size();
        int numMembers = members.size();
        int base = totalPartitions / numMembers;
        int extra = totalPartitions % numMembers;

        PriorityQueue<MemberSlot> heap = new PriorityQueue<>((a, b) -> {
            if (a.load != b.load) {
                return Integer.compare(a.load, b.load);
            }
            return Integer.compare(a.index, b.index);
        });

        for (int i = 0; i < numMembers; i++) {
            int desired = base + (i < extra ? 1 : 0);
            heap.add(new MemberSlot(i, members.get(i), 0, desired));
        }

        for (TopicPartition tp : allPartitions) {
            MemberSlot slot = heap.poll();
            String member = slot.memberId;

            result.get(member)
                  .computeIfAbsent(tp.getTopic(), t -> new ArrayList<>())
                  .add(tp.getPartition());

            slot.load++;
            if (slot.load < slot.desired) {
                heap.add(slot);
            }
        }

        for (Map<String, List<Integer>> byTopic : result.values()) {
            for (Map.Entry<String, List<Integer>> e : byTopic.entrySet()) {
                Collections.sort(e.getValue());
                e.setValue(Collections.unmodifiableList(e.getValue()));
            }
        }

        return result;
    }

    private List<TopicPartition> buildPartitionUniverse(
            List<String> topics,
            Map<String, Integer> topicToPartitionCount
    ) {
        List<TopicPartition> all = new ArrayList<>();
        for (String topic : topics) {
            int count = topicToPartitionCount.getOrDefault(topic, 0);
            for (int p = 0; p < count; p++) {
                all.add(new TopicPartition(topic, p));
            }
        }
        return all;
    }

    private static class MemberSlot {
        final int index;
        final String memberId;
        int load;
        final int desired;

        MemberSlot(int index, String memberId, int load, int desired) {
            this.index = index;
            this.memberId = memberId;
            this.load = load;
            this.desired = desired;
        }
    }
}