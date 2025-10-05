package consumer.assignors;

import java.util.*;


// TODO: Worthwhile to make into its own story for later. Just becuz future research is needed.
// TODO: I will document what I found however
/*
    This is the HIGH LEVEL algo/steps from good mutually agreed from both LLMs and Youtube Confluent.

    GOALS:
        1) Balanced: total partitions are split so each member has either ⌊P/M⌋ or ⌈P/M⌉.
        2) Sticky: keep as many prior placements as possible; move only what’s necessary to reach (1).
         Deterministic: same inputs → same output.

    Data Structures:
          - taken: HashSet<TopicPartition> of partitions already kept/assigned (prevents duplicates).
          - unassigned: ArrayDeque<TopicPartition> (FIFO) holding partitions that must be (re)assigned.
          - result: Map<memberId, Map<topic, List<Integer>>> accumulating the plan.
          - desired[]: int[M] target load per member (balanced totals).
          - load[]:    int[M] current load per member after “keep” step.
          - pq: PriorityQueue<MemberSlot> — a min-heap ordered by (load ASC, memberIndex ASC).
          -  MemberSlot { index:int, load:int, desired:int }.

   STEPS:
           0) Normalize for determinism:
                - Sort memberIds; sort topics.

             1) Build the partition universe:
                - allPartitions := sorted list of every (topic, partitionId) that exists now.

             2) Compute balanced targets:
                - total P = allPartitions.size(), members M = memberIds.size()
                - base = P / M, extra = P % M
                - desired[i] = base + (i < extra ? 1 : 0)  // first ‘extra’ members get one more

             3) Keep valid previous assignments (stickiness pass):
                - For each member in sorted order:
                    For each (topic -> partitions) previously owned:
                      For each partition p:
                        If topic still exists AND 0 <= p < topicPartitionCount[topic]
                        AND (topic,p) not in taken:
                          - Append p to result[member][topic], mark taken.add((topic,p)).
                - This preserves as much prior placement as possible without violating current realities.

             4) Measure current load & collect unassigned:
                - load[i] = sum of partitions already in result for member i.
                - unassigned = every (topic,partition) in allPartitions that is NOT in taken.

             5) Evict from overfull members (to achieve balance with minimal churn):
                - For each member i:
                    over = load[i] - desired[i]
                    If over > 0:
                      - Gather that member’s owned TopicPartitions (flatten & sort deterministically).
                      - Remove ‘over’ partitions from the END (highest (topic,partition) first) to be victims.
                      - For each victim:
                          * Remove from result[member]
                          * taken.remove(victim)
                          * unassigned.addLast(victim)
                          * load[i]--
                - Eviction is deterministic and limited to what’s strictly necessary.

             6) Fill underfull members using a MIN-HEAP (the “heap” part):
                - Initialize pq with MemberSlot(i, load[i], desired[i]) for any i where load[i] < desired[i].
                - While unassigned not empty AND pq not empty:
                    tp = unassigned.pollFirst()         // pick next unassigned partition in stable order
                    ms = pq.poll()                      // pop least-loaded member (min-heap by load, then index)
                    assign tp to memberIds[ms.index]    // result[member][tp.topic].add(tp.partition)
                    ms.load++
                    If ms.load < ms.desired:
                       pq.add(ms)                       // still needs more; push back with updated load
                - The min-heap ensures each partition goes to the *currently* least-loaded member,
                  guaranteeing balance while minimizing further movement.

             7) Canonicalize & freeze:
                - Sort each member’s per-topic partition lists; optionally wrap them unmodifiable.

         Edge Cases:
          - No members or no partitions → empty assignment map.
          - More members than partitions → some members end at 0 (desired[i] may be 0).
          - Topic shrinks or disappears → invalid prior partitions are ignored automatically in step 3.
 */

public class StickyAssignor implements PartitionAssignor {
    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        throw new UnsupportedOperationException("StickyAssignor not implemented yet");
    }
}