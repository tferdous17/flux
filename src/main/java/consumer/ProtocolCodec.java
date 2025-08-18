package consumer;
import com.google.protobuf.ByteString;
import proto.Assignment;
import proto.ProtocolMetadata;

import java.nio.charset.StandardCharsets;
import java.util.*;

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


    // TODO: RESEARCH ENCODING/DECODING perhaps use Kyro again?
    static byte[] encodeMetadata(Collection<String> topics, String... pairs) {
        return "".getBytes((StandardCharsets.UTF_8));
    }

    static byte[] encodeAssignment(Map<String, List<Integer>> topicPartitions) {
        return "".getBytes((StandardCharsets.UTF_8));
    }

    static Map<String, List<Integer>> decodeAssignment(byte[] bytes) {
        return new LinkedHashMap<>();
    }
}
