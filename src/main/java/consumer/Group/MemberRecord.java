package consumer.Group;

import proto.ProtocolMetadata;

import java.util.List;
import java.util.Map;

public class MemberRecord {
    public final String memberId;
    public volatile long lastHeartbeatMs;
    public List<String> protocolPreferenceOrder = List.of();
    public Map<String, ProtocolMetadata> protocolsByName = Map.of();

    public MemberRecord(String memberId) {
        this.memberId = memberId;
    }
}
