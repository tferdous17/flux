package consumer.assignors;

import consumer.assignors.PartitionAssignor;

import java.util.*;

/**
 * Range assignor: partitions for each topic are split into contiguous ranges
 * across members. If N partitions and M members:
 *  - each member gets either floor(N/M) or ceil(N/M) partitions;
 *  - earlier members get the extra one when N % M != 0.
 */
public final class RangeAssignor implements PartitionAssignor {

    @Override
    public Map<String, Map<String, List<Integer>>> assign(
            List<String> memberIds,
            Map<String, Integer> topicToPartitionCount
    ) {
        if (memberIds == null || memberIds.isEmpty()) {
            throw new IllegalArgumentException("memberIds must not be empty");
        }
        // Initialize output: ensure every member has a (possibly empty) topic map
        Map<String, Map<String, List<Integer>>> full = new LinkedHashMap<>();
        for (String mid : memberIds) {
            full.put(mid, new LinkedHashMap<>());
        }

        if (topicToPartitionCount == null || topicToPartitionCount.isEmpty()) {
            return full; // nothing to assign
        }

        final int m = memberIds.size();

        for (Map.Entry<String, Integer> e : topicToPartitionCount.entrySet()) {
            String topic = e.getKey();
            int partitions = Math.max(0, e.getValue() == null ? 0 : e.getValue());

            // Compute contiguous ranges per member
            int base = (m == 0) ? 0 : (partitions / m);
            int rem  = (m == 0) ? 0 : (partitions % m);

            int cursor = 0;
            for (int i = 0; i < m; i++) {
                int size = base + (i < rem ? 1 : 0); // first 'rem' members get one extra
                List<Integer> slice = new ArrayList<>(size);
                for (int k = 0; k < size; k++) {
                    slice.add(cursor + k);
                }
                cursor += size;

                if (!slice.isEmpty()) {
                    full.get(memberIds.get(i))
                            .computeIfAbsent(topic, t -> new ArrayList<>())
                            .addAll(slice);
                }
            }
            // If partitions < members, some members simply get none for this topic (OK).
        }

        return full;
    }
}
