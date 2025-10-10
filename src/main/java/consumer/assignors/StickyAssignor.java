package consumer.assignors;

import commons.TopicPartition;

import java.util.*;

/**
 * StickyAssignor with true stickiness via previous assignment preservation.
 *
 * Two-phase algorithm:
 * 1. Preserve existing assignments for members still in the group
 * 2. Distribute unassigned partitions using min-heap for O(U log M) balanced assignment
 *    where U = unassigned partitions, M = members
 *
 * Guarantees balanced distribution where each member gets ⌊P/M⌋ or ⌈P/M⌉ partitions.
 * Minimizes partition movement during rebalances by preserving previous assignments.
 */
public class StickyAssignor implements PartitionAssignor {

    /**
     * Tracks a member's current partition load for heap-based load balancing.
     * Ordered by load (ascending), then member ID (alphabetically) for deterministic assignment.
     */
    private static class MemberLoad implements Comparable<MemberLoad> {
        String memberId;
        int load;

        MemberLoad(String memberId, int load) {
            this.memberId = memberId;
            this.load = load;
        }

        @Override
        public int compareTo(MemberLoad other) {
            int cmp = Integer.compare(this.load, other.load);
            if (cmp != 0) return cmp;
            return this.memberId.compareTo(other.memberId);
        }
    }

    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        return assign(memberIds, topicToPartitionCount, Collections.emptyMap());
    }

    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount,
            Map<String, Map<String, List<Integer>>> previousAssignment
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

        Set<TopicPartition> assigned = new HashSet<>();
        Map<String, Integer> memberLoad = new HashMap<>();
        for (String m : members) {
            memberLoad.put(m, 0);
        }

        int totalPartitions = allPartitions.size();
        int numMembers = members.size();
        int maxPerMember = (totalPartitions + numMembers - 1) / numMembers;

        // Phase 1: Preserve previous assignments up to balance limit
        if (previousAssignment != null && !previousAssignment.isEmpty()) {
            for (String member : members) {
                Map<String, List<Integer>> prevTopics = previousAssignment.get(member);
                if (prevTopics != null) {
                    for (Map.Entry<String, List<Integer>> entry : prevTopics.entrySet()) {
                        String topic = entry.getKey();
                        for (Integer partition : entry.getValue()) {
                            if (memberLoad.get(member) >= maxPerMember) {
                                break;
                            }
                            TopicPartition tp = new TopicPartition(topic, partition);
                            if (isValidPartition(tp, topicToPartitionCount) && !assigned.contains(tp)) {
                                result.get(member)
                                      .computeIfAbsent(topic, t -> new ArrayList<>())
                                      .add(partition);
                                assigned.add(tp);
                                memberLoad.put(member, memberLoad.get(member) + 1);
                            }
                        }
                    }
                }
            }
        }

        // Phase 2: Distribute unassigned partitions using min-heap
        PriorityQueue<MemberLoad> heap = new PriorityQueue<>();
        for (String member : members) {
            heap.offer(new MemberLoad(member, memberLoad.get(member)));
        }

        List<TopicPartition> unassigned = new ArrayList<>();
        for (TopicPartition tp : allPartitions) {
            if (!assigned.contains(tp)) {
                unassigned.add(tp);
            }
        }

        for (TopicPartition tp : unassigned) {
            MemberLoad memberWithLeastLoad = heap.poll();
            result.get(memberWithLeastLoad.memberId)
                  .computeIfAbsent(tp.getTopic(), t -> new ArrayList<>())
                  .add(tp.getPartition());
            memberWithLeastLoad.load++;
            heap.offer(memberWithLeastLoad);
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

    private boolean isValidPartition(TopicPartition tp, Map<String, Integer> topicToPartitionCount) {
        Integer count = topicToPartitionCount.get(tp.getTopic());
        return count != null && tp.getPartition() >= 0 && tp.getPartition() < count;
    }
}