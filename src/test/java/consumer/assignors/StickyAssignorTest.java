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
