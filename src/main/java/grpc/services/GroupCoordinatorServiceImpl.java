package grpc.services;

import consumer.Group.GroupState;
import consumer.Group.MemberRecord;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import proto.*;
import server.internal.Broker;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class GroupCoordinatorServiceImpl extends GroupCoordinatorServiceGrpc.GroupCoordinatorServiceImplBase {
    Broker broker;

    // Per-group coordinator state (simple in-memory)
    private final Map<String, GroupState> groups = new ConcurrentHashMap<>();

    public GroupCoordinatorServiceImpl(Broker broker){
        this.broker = broker;
    }

    /*
        1) Figure out the leader
            1.1) the first member to join the group after the rebalance round is typically the leader
            1.2) if that member drops however or fails a heartbeat, the coordinator will elect a new leader at the next rebalance
        2) Figure out the protocol choice
            2.1) Compute the intersection of all protocols from followers.
            2.2) Choose protocol by leader's preference
        3) create the partition mapping

        We are still missing production stuff that I listed below but this indeed alot of work for single ticket.
        TODO:
            - Waiting for all members to join
            - Heartbeat-driven liveness checks
            - Generation-based staleness checks,
            - Proper error handling for edge cases.
            Improvements:
                - A state machine for group rebalancing.
                - Background heartbeat checks.
                - Timeout-driven rebalance completion.
                - Generation ID management.
     */
    @Override
    public void joinGroup(JoinGroupRequest req, StreamObserver<JoinGroupResponse> responseObserver) {
        final String groupId = req.getGroupId();
        final GroupState group = groups.computeIfAbsent(groupId, k -> new GroupState());

        String suppliedMemberId = req.getMemberId();
        if (suppliedMemberId.isBlank()) {
            suppliedMemberId = newMemberId();
        }
        final String memberId = suppliedMemberId;

        final List<String> protocolOrderForThisMember = req.getProtocolsList().stream()
                .map(ProtocolMetadata::getName)
                .collect(Collectors.toList()
        );

        if (protocolOrderForThisMember.isEmpty()) {
            respondError(responseObserver, GroupStatus.INCOMPATIBLE_PROTOCOL);
            return;
        }

        JoinGroupResponse response;
        synchronized (group.lock) {
            // Start or restart a rebalance round if none in progress
            if (!group.rebalanceInProgress) {
                group.rebalanceInProgress = true;
                group.generationId++;
                group.protocolChosen = null;
                group.leaderId = null;
                group.members.clear();
                group.roundStartedAt = Instant.now();
                group.roundTimeoutMs = Math.max(1000, req.getRebalanceTimeoutMs());
            }

            // Register/update member for this round
            MemberRecord rec = group.members.computeIfAbsent(memberId, MemberRecord::new);
            rec.protocolPreferenceOrder = protocolOrderForThisMember;
            rec.protocolsByName = req.getProtocolsList().stream()
                    .collect(Collectors.toMap(ProtocolMetadata::getName, pm -> pm)
            );
            rec.lastHeartbeatMs = System.currentTimeMillis();

            // Elect leader if none yet (first joiner in this round)
            if (group.leaderId == null) {
                group.leaderId = memberId;
            }

            // Compute intersection of protocol names across current joiners
            Set<String> intersection = null;
            for (MemberRecord m : group.members.values()) {
                Set<String> names = m.protocolsByName.keySet();
                if (intersection == null) intersection = new HashSet<>(names);
                else {
                    intersection.retainAll(names);
                    if (intersection.isEmpty()) break;
                }
            }
            if (intersection == null || intersection.isEmpty()) {
                respondError(responseObserver, GroupStatus.INCOMPATIBLE_PROTOCOL);
                return;
            }

            // Choose protocol by leader's preference
            MemberRecord leader = group.members.get(group.leaderId);
            String chosen = null;
            for (String p : leader.protocolPreferenceOrder) {
                if (intersection.contains(p)) { chosen = p; break; }
            }
            if (chosen == null) {
                chosen = intersection.stream().sorted().findFirst().orElse(""); // deterministic fallback
            }
            if (chosen.isEmpty()) {
                respondError(responseObserver, GroupStatus.INCOMPATIBLE_PROTOCOL);
                return;
            }
            group.protocolChosen = chosen;

            // Build response (leader gets members[], followers do not)
            JoinGroupResponse.Builder b = JoinGroupResponse.newBuilder()
                    .setStatus(GroupStatus.GROUP_OK)
                    .setMemberId(memberId)
                    .setGenerationId(group.generationId)
                    .setLeaderId(group.leaderId)
                    .setProtocol(group.protocolChosen);

            if (memberId.equals(group.leaderId)) {
                for (MemberRecord m : group.members.values()) {
                    ProtocolMetadata pm = m.protocolsByName.get(group.protocolChosen);
                    MemberInfo.Builder mi = MemberInfo.newBuilder()
                            .setMemberId(m.memberId)
                            .setMetadata(pm != null ? pm.getMetadata() : ByteString.EMPTY);
                    b.addMembers(mi.build());
                }
            }

            // Note: a production coordinator would hold JoinGroup until the round closes / timeout,
            // then reply to all joiners. This simplified version replies immediately.
            response = b.build();
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }


    private static String newMemberId() {
        return "member-" + UUID.randomUUID();
    }

    private static void respondError(StreamObserver<JoinGroupResponse> obs, GroupStatus status) {
        JoinGroupResponse resp = JoinGroupResponse.newBuilder()
                .setStatus(status)
                .setMemberId("")
                .setGenerationId(-1)
                .setLeaderId("")
                .setProtocol("")
                .build();
        obs.onNext(resp);
        obs.onCompleted();
    }
}
