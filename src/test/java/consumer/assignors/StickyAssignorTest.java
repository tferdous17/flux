package consumer.assignors;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class StickyAssignorTest {

    private final StickyAssignor assignor = new StickyAssignor();

    @Test
    void emptyInputs_returnEmpty() {
        assertTrue(assignor.assign(Collections.emptyList(), Map.of("t", 3)).isEmpty());
        assertTrue(assignor.assign(List.of("m1"), Collections.emptyMap()).isEmpty());
        assertTrue(assignor.assign(Collections.emptyList(), Collections.emptyMap()).isEmpty());
        assertTrue(assignor.assign(null, Map.of("t", 1)).isEmpty());
    }

    @Test
    void singleTopic_evenDistribution() {
        List<String> members = List.of("c", "a", "b");
        Map<String, Integer> counts = Map.of("t", 6);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        assertEquals(2, r.get("a").get("t").size());
        assertEquals(2, r.get("b").get("t").size());
        assertEquals(2, r.get("c").get("t").size());
        assertEquals(Set.of(0, 1, 2, 3, 4, 5), ownedSet(r, "t"));
    }

    @Test
    void singleTopic_unevenDistribution() {
        List<String> members = List.of("c", "a", "b");
        Map<String, Integer> counts = Map.of("t", 5);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        int total = totalOwned(r, "a") + totalOwned(r, "b") + totalOwned(r, "c");
        assertEquals(5, total);

        int minOwned = Math.min(Math.min(totalOwned(r, "a"), totalOwned(r, "b")), totalOwned(r, "c"));
        int maxOwned = Math.max(Math.max(totalOwned(r, "a"), totalOwned(r, "b")), totalOwned(r, "c"));
        assertEquals(1, maxOwned - minOwned);
    }

    @Test
    void multipleTopics() {
        List<String> members = List.of("c", "b", "a");
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("bar", 2);
        counts.put("foo", 5);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        assertEquals(Set.of(0, 1, 2, 3, 4), ownedSet(r, "foo"));
        assertEquals(Set.of(0, 1), ownedSet(r, "bar"));

        int total = totalOwned(r, "a") + totalOwned(r, "b") + totalOwned(r, "c");
        assertEquals(7, total);
    }

    @Test
    void moreMembersThanPartitions() {
        List<String> members = List.of("D", "C", "B", "A");
        Map<String, Integer> counts = Map.of("t", 2);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        int assigned = 0;
        int unassigned = 0;
        for (String m : members) {
            if (totalOwned(r, m) > 0) assigned++;
            else unassigned++;
        }

        assertEquals(2, assigned);
        assertEquals(2, unassigned);
    }

    @Test
    void balanceProperty() {
        List<String> members = List.of("a", "b", "c");
        Map<String, Integer> counts = Map.of("A", 5, "B", 2, "C", 4);

        int totalP = counts.values().stream().mapToInt(i -> i).sum();
        int M = members.size();

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        for (String m : members) {
            int owned = totalOwned(r, m);
            int base = totalP / M;
            int ceil = (totalP + M - 1) / M;
            assertTrue(owned == base || owned == ceil,
                    "member " + m + " owns " + owned + " not in {" + base + "," + ceil + "}");
        }
    }

    @Test
    void determinism() {
        List<String> members = List.of("x", "y", "z");
        Map<String, Integer> counts = Map.of("alpha", 4, "beta", 3);

        var r1 = assignor.assign(members, counts);
        var r2 = assignor.assign(members, counts);

        assertEquals(r1, r2);
    }

    @Test
    void listsAreUnmodifiable() {
        List<String> members = List.of("m1", "m2");
        Map<String, Integer> counts = Map.of("t", 3);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        assertThrows(UnsupportedOperationException.class, () -> r.get("m1").get("t").add(99));
    }

    @Test
    void trueStickiness_memberLeaves() {
        List<String> members3 = List.of("a", "b", "c");
        Map<String, Integer> counts = Map.of("topic1", 9);

        Map<String, Map<String, List<Integer>>> r1 = assignor.assign(members3, counts, Collections.emptyMap());

        assertEquals(3, r1.get("a").get("topic1").size());
        assertEquals(3, r1.get("b").get("topic1").size());
        assertEquals(3, r1.get("c").get("topic1").size());

        List<String> members2 = List.of("a", "b");
        Map<String, Map<String, List<Integer>>> r2 = assignor.assign(members2, counts, r1);

        List<Integer> aPartitions1 = r1.get("a").get("topic1");
        List<Integer> bPartitions1 = r1.get("b").get("topic1");
        List<Integer> aPartitions2 = r2.get("a").get("topic1");
        List<Integer> bPartitions2 = r2.get("b").get("topic1");

        assertTrue(aPartitions2.containsAll(aPartitions1), "a should keep all previous partitions");
        assertTrue(bPartitions2.containsAll(bPartitions1), "b should keep all previous partitions");

        assertEquals(9, aPartitions2.size() + bPartitions2.size());
        assertTrue(Math.abs(aPartitions2.size() - bPartitions2.size()) <= 1);
    }

    @Test
    void trueStickiness_memberJoins() {
        List<String> members2 = List.of("a", "b");
        Map<String, Integer> counts = Map.of("topic1", 9);

        Map<String, Map<String, List<Integer>>> r1 = assignor.assign(members2, counts, Collections.emptyMap());

        assertEquals(9, r1.get("a").get("topic1").size() + r1.get("b").get("topic1").size());

        List<String> members3 = List.of("a", "b", "c");
        Map<String, Map<String, List<Integer>>> r2 = assignor.assign(members3, counts, r1);

        List<Integer> aPartitions1 = r1.get("a").get("topic1");
        List<Integer> bPartitions1 = r1.get("b").get("topic1");

        Map<String, List<Integer>> aTopics2 = r2.get("a");
        Map<String, List<Integer>> bTopics2 = r2.get("b");
        List<Integer> aPartitions2 = aTopics2 != null ? aTopics2.get("topic1") : null;
        List<Integer> bPartitions2 = bTopics2 != null ? bTopics2.get("topic1") : null;

        if (aPartitions2 != null && bPartitions2 != null) {
            Set<Integer> aRetained = new HashSet<>(aPartitions1);
            aRetained.retainAll(aPartitions2);
            Set<Integer> bRetained = new HashSet<>(bPartitions1);
            bRetained.retainAll(bPartitions2);

            int totalRetained = aRetained.size() + bRetained.size();
            assertTrue(totalRetained >= 3, "Some partitions should stay with original members");
        }

        int total = totalOwned(r2, "a") + totalOwned(r2, "b") + totalOwned(r2, "c");
        assertEquals(9, total);
        assertTrue(totalOwned(r2, "c") >= 2, "New member should get partitions");
    }

    @Test
    void trueStickiness_minimalMovement() {
        List<String> members = List.of("a", "b", "c");
        Map<String, Integer> counts = Map.of("t1", 6, "t2", 6);

        Map<String, Map<String, List<Integer>>> r1 = assignor.assign(members, counts, Collections.emptyMap());

        assertEquals(12, totalOwned(r1, "a") + totalOwned(r1, "b") + totalOwned(r1, "c"));

        Map<String, Map<String, List<Integer>>> r2 = assignor.assign(members, counts, r1);

        assertEquals(r1, r2, "Reassignment with same members should be identical");
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

    private static Set<Integer> ownedSet(Map<String, Map<String, List<Integer>>> r, String topic) {
        Set<Integer> s = new HashSet<>();
        for (Map<String, List<Integer>> byTopic : r.values()) {
            List<Integer> lst = byTopic.get(topic);
            if (lst != null) s.addAll(lst);
        }
        return s;
    }
}
