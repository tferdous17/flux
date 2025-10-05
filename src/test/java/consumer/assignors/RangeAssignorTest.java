package consumer.assignors;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class RangeAssignorTest {

    private final RangeAssignor assignor = new RangeAssignor();

    @Test
    void emptyInputs_returnEmpty() {
        assertTrue(assignor.assign(Collections.emptyList(), Map.of("t", 3)).isEmpty());
        assertTrue(assignor.assign(List.of("m1"), Collections.emptyMap()).isEmpty());
        assertTrue(assignor.assign(Collections.emptyList(), Collections.emptyMap()).isEmpty());
        assertTrue(assignor.assign(null, Map.of("t", 1)).isEmpty());
    }

    @Test
    void singleTopic_evenlyDivisible_contiguousRanges() {
        // 6 partitions, 3 members -> each gets 2: [0,1], [2,3], [4,5]
        List<String> members = List.of("c", "a", "b"); // unsorted on purpose
        Map<String, Integer> counts = Map.of("t", 6);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        assertEquals(List.of(0, 1), r.get("a").get("t"));
        assertEquals(List.of(2, 3), r.get("b").get("t"));
        assertEquals(List.of(4, 5), r.get("c").get("t"));

        assertTrue(isContiguous(r.get("a").get("t")));
        assertTrue(isContiguous(r.get("b").get("t")));
        assertTrue(isContiguous(r.get("c").get("t")));
        assertEquals(Set.of(0,1,2,3,4,5), ownedSet(r, "t"));
    }

    @Test
    void singleTopic_notEven_firstExtraMembersGetOneMore() {
        // 5 partitions, 3 members => base=1, extra=2 -> a:2, b:2, c:1
        List<String> members = List.of("c", "a", "b");
        Map<String, Integer> counts = Map.of("t", 5);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        // members sorted: [a,b,c]; nextStart runs left->right
        assertEquals(List.of(0, 1), r.get("a").get("t"));
        assertEquals(List.of(2, 3), r.get("b").get("t"));
        assertEquals(List.of(4),     r.get("c").get("t"));

        assertTrue(isContiguous(r.get("a").get("t")));
        assertTrue(isContiguous(r.get("b").get("t")));
        assertTrue(isContiguous(r.get("c").get("t")));
        assertEquals(Set.of(0,1,2,3,4), ownedSet(r, "t"));
    }

    @Test
    void multipleTopics_independentRangesPerTopic() {
        // topics sorted: bar, foo
        // foo has 5 parts -> a:2, b:2, c:1
        // bar has 2 parts -> a:1, b:1, c:0
        List<String> members = List.of("c", "b", "a");
        Map<String, Integer> counts = new LinkedHashMap<>();
        counts.put("bar", 2);
        counts.put("foo", 5);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        // foo (5 → a:[0,1], b:[2,3], c:[4])
        assertEquals(List.of(0, 1), r.get("a").get("foo"));
        assertEquals(List.of(2, 3), r.get("b").get("foo"));
        assertEquals(List.of(4),     r.get("c").get("foo"));

        // bar (2 → a:[0], b:[1], c:[])
        assertEquals(List.of(0), r.get("a").get("bar"));
        assertEquals(List.of(1), r.get("b").get("bar"));
        assertFalse(r.get("c").containsKey("bar"));

        // contiguity checks
        assertTrue(isContiguous(r.get("a").get("foo")));
        assertTrue(isContiguous(r.get("b").get("foo")));
        assertTrue(isContiguous(r.get("c").get("foo")));
        assertTrue(isContiguous(r.get("a").get("bar")));
        assertTrue(isContiguous(r.get("b").get("bar")));

        //  every partition assigned exactly once per topic
        assertEquals(Set.of(0,1,2,3,4), ownedSet(r, "foo"));
        assertEquals(Set.of(0,1), ownedSet(r, "bar"));
    }


    @Test
    void moreMembersThanPartitions_someMembersGetNone() {
        List<String> members = List.of("D", "C", "B", "A"); // will sort to A,B,C,D
        Map<String, Integer> counts = Map.of("t", 2);

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        assertEquals(List.of(0), r.get("A").get("t"));
        assertEquals(List.of(1), r.get("B").get("t"));
        assertTrue(r.get("C").isEmpty());
        assertTrue(r.get("D").isEmpty());
    }

    @Test
    void determinism_sameInputs_sameOutputs() {
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
    void balancePerTopic_firstExtrasGetTheRemainder() {
        List<String> members = List.of("a", "b", "c");
        Map<String, Integer> counts = Map.of("A", 7, "B", 2); // A: base=2, extra=1 -> [3,2,2]; B: base=0, extra=2 -> [1,1,0]

        Map<String, Map<String, List<Integer>>> r = assignor.assign(members, counts);

        // A
        assertEquals(3, sizeOrZero(r.get("a").get("A")));
        assertEquals(2, sizeOrZero(r.get("b").get("A")));
        assertEquals(2, sizeOrZero(r.get("c").get("A")));
        // B
        assertEquals(1, sizeOrZero(r.get("a").get("B")));
        assertEquals(1, sizeOrZero(r.get("b").get("B")));
        assertEquals(0, sizeOrZero(r.get("c").get("B")));
    }

    // HELPERS
    private static boolean isContiguous(List<Integer> parts) {
        if (parts == null || parts.isEmpty()) return true;
        List<Integer> tmp = new ArrayList<>(parts);
        Collections.sort(tmp);
        for (int i = 1; i < tmp.size(); i++) {
            if (tmp.get(i) != tmp.get(i - 1) + 1) return false;
        }
        return true;
    }

    private static Set<Integer> ownedSet(Map<String, Map<String, List<Integer>>> r, String topic) {
        Set<Integer> s = new HashSet<>();
        for (Map<String, List<Integer>> byTopic : r.values()) {
            List<Integer> lst = byTopic.get(topic);
            if (lst != null) s.addAll(lst);
        }
        return s;
    }

    private static int sizeOrZero(List<Integer> lst) {
        return lst == null ? 0 : lst.size();
    }
}
