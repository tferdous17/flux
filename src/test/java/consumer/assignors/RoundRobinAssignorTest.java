package consumer.assignors;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class RoundRobinAssignorTest {

    private final RoundRobinAssignor assignor = new RoundRobinAssignor();

    @Test
    void emptyMembersOrTopics_returnsEmpty() {
        Map<String, Map<String, List<Integer>>> a =
                assignor.assign(Collections.emptyList(), Map.of("t", 3));
        Map<String, Map<String, List<Integer>>> b = assignor.assign(List.of("m1", "m2"), Collections.emptyMap());
        assertTrue(a.isEmpty());
        assertTrue(b.isEmpty());
    }

    @Test
    void singleTopic_evenDistribution_roundRobinOrder() {
        List<String> members = List.of("c", "a", "b"); // UNSORTED
        Map<String, Integer> topicCounts = Map.of("orders", 6);

        Map<String, Map<String, List<Integer>>> result = assignor.assign(members, topicCounts);

        // members are sorted inside assignor → [a, b, c]
        assertEquals(List.of(0, 3), result.get("a").get("orders"));
        assertEquals(List.of(1, 4), result.get("b").get("orders"));
        assertEquals(List.of(2, 5), result.get("c").get("orders"));

        // each member gets exactly 2
        assertEquals(2, totalOwned(result, "a"));
        assertEquals(2, totalOwned(result, "b"));
        assertEquals(2, totalOwned(result, "c"));
    }

    @Test
    void multipleTopics_respectsSortedTopicThenPartitionOrder() {
        // topics will be iterated in alpha, then beta
        Map<String, Integer> topicCounts = new LinkedHashMap<>();
        topicCounts.put("beta", 3);
        topicCounts.put("alpha", 2);

        List<String> members = List.of("m2", "m1"); // UNSORTED

        Map<String, Map<String, List<Integer>>> result =
                assignor.assign(members, topicCounts);

        // deterministic member order is [m1, m2]
        // flattened partitions in order: alpha-0, alpha-1, beta-0, beta-1, beta-2
        // deal to m1, m2, m1, m2, m1
        assertEquals(List.of(0), result.get("m1").get("alpha"));
        assertEquals(List.of(1), result.get("m2").get("alpha"));
        assertEquals(List.of(0, 2), result.get("m1").get("beta"));
        assertEquals(List.of(1), result.get("m2").get("beta"));

        assertEquals(3, totalOwned(result, "m1"));
        assertEquals(2, totalOwned(result, "m2"));
    }

    @Test
    void moreMembersThanPartitions_someMembersGetZero() {
        List<String> members = List.of("A", "B", "C", "D");
        Map<String, Integer> topics = Map.of("t", 2);

        Map<String, Map<String, List<Integer>>> result =
                assignor.assign(members, topics);

        // sorted members: A, B, C, D
        assertEquals(List.of(0), result.get("A").get("t"));
        assertEquals(List.of(1), result.get("B").get("t"));
        assertTrue(result.get("C").isEmpty());
        assertTrue(result.get("D").isEmpty());
    }

    @Test
    void determinism_sameInputs_sameOutputs() {
        List<String> members = List.of("x", "y", "z");
        Map<String, Integer> topics = Map.of("a", 2, "b", 4, "c", 1);

        Map<String, Map<String, List<Integer>>> r1 = assignor.assign(members, topics);
        Map<String, Map<String, List<Integer>>> r2 = assignor.assign(members, topics);

        assertEquals(r1, r2);
    }

    @Test
    void listsAreUnmodifiable() {
        List<String> members = List.of("m1", "m2");
        Map<String, Integer> topics = Map.of("t", 3);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, topics);
        List<Integer> m1List = r.get("m1").get("t");

        assertThrows(UnsupportedOperationException.class, () -> m1List.add(99));
    }

    @Test
    void balanceProperty_eachMemberGetsFloorOrCeil() {
        List<String> members = List.of("a", "b", "c");
        Map<String, Integer> topics = Map.of(
                "A", 5,
                "B", 2,
                "C", 4
        );
        int totalP = topics.values().stream().mapToInt(i -> i).sum(); // 11
        int M = members.size();
        int base = totalP / M;   // 11/3 = 3
        int ceil = (totalP + M - 1) / M; // 4

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, topics);

        for (String m : members) {
            int owned = totalOwned(r, m);
            assertTrue(owned == base || owned == ceil,
                    () -> "member " + m + " owns " + owned + " which is not in {"
                            + base + "," + ceil + "}");
        }
    }
    private static int totalOwned(Map<String, Map<String, List<Integer>>> result, String member) {
        Map<String, List<Integer>> byTopic = result.get(member);
        if (byTopic == null) return 0;
        int sum = 0;
        for (List<Integer> lst : byTopic.values()) {
            sum += lst.size();
        }
        return sum;
    }
}
