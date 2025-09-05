package consumer.Group;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class GroupState {
    public final Object lock = new Object();

    public int generationId = 0;
    public boolean rebalanceInProgress = false;

    public String leaderId = null;       // elected on first join of the round
    public String protocolChosen = null; // chosen via leader's preference

    public final Map<String, MemberRecord> members = new HashMap<>();

    public Instant roundStartedAt = Instant.EPOCH;
    public int roundTimeoutMs = 10000;
}
