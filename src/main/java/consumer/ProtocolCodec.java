package consumer;
import com.google.protobuf.ByteString;
import proto.Assignment;
import proto.ProtocolMetadata;

import java.nio.charset.StandardCharsets;
import java.util.*;

// TODO: RESEARCH ENCODING/DECODING perhaps use Kyro again? (Everything Below)
public final class ProtocolCodec {

    public static ProtocolMetadata buildProtocolMetadata(Collection<String> topics, String assignor, String rack) {
        byte[] md = encodeMetadata(topics, "assignor=" + assignor, "clientRack=" + rack);
        return ProtocolMetadata.newBuilder()
                .setName(assignor)
                .setMetadata(ByteString.copyFrom(md))
                .build();
    }

    public static Assignment packAssignment(Map<String, List<Integer>> topicPartitions) {
        byte[] bytes = encodeAssignment(topicPartitions);
        return Assignment.newBuilder()
                .setAssignment(ByteString.copyFrom(bytes))
                .build();
    }

    public static Map<String, List<Integer>> unpackAssignment(Assignment a) {
        return decodeAssignment(a.getAssignment().toByteArray());
    }

    static byte[] encodeMetadata(Collection<String> topics, String... pairs) {
        return "".getBytes((StandardCharsets.UTF_8));
    }
    static byte[] encodeAssignment(Map<String, List<Integer>> topicPartitions) {
        return "".getBytes((StandardCharsets.UTF_8));
    }
    static byte[] encodeFullGroupAssignment(Map<String, Map<String, List<Integer>>> full) {
        StringBuilder sb = new StringBuilder();
        boolean firstMember = true;

        for (Map.Entry<String, Map<String, List<Integer>>> me : full.entrySet()) {
            if (!firstMember) sb.append("||");
            firstMember = false;

            String memberId = me.getKey();
            sb.append(memberId == null ? "" : memberId).append(':');

            Map<String, List<Integer>> tp = me.getValue();
            if (tp == null || tp.isEmpty()) continue;

            boolean firstTopic = true;
            for (Map.Entry<String, List<Integer>> te : tp.entrySet()) {
                if (!firstTopic) sb.append(';');
                firstTopic = false;

                String topic = te.getKey() == null ? "" : te.getKey();
                sb.append(topic).append('=');

                List<Integer> parts = te.getValue() == null ? Collections.emptyList()
                        : new ArrayList<>(te.getValue());
                Collections.sort(parts);
                for (int i = 0; i < parts.size(); i++) {
                    if (i > 0) sb.append(',');
                    sb.append(parts.get(i));
                }
            }
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }
    static Map<String, List<Integer>> decodeAssignment(byte[] bytes) {
        return new LinkedHashMap<>();
    }
}
