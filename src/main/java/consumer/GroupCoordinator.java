package consumer;

import proto.GroupCoordinatorServiceGrpc;
import proto.JoinGroupRequest;
import proto.JoinGroupResponse;
import proto.ProtocolMetadata;

import java.util.List;

public final class GroupCoordinator {
    private final GroupCoordinatorServiceGrpc.GroupCoordinatorServiceBlockingStub stub;

    public GroupCoordinator(GroupCoordinatorServiceGrpc.GroupCoordinatorServiceBlockingStub stub) {
        this.stub = stub;
    }

    public JoinGroupResponse joinGroupLoop(String groupId,
                                           String memberId,
                                           int sessionTimeoutMs,
                                           int rebalanceTimeoutMs,
                                           List<ProtocolMetadata> protocols) {
        long backoffMs = 200;

       while (true) {
            JoinGroupRequest request = JoinGroupRequest.newBuilder()
                    .setGroupId(groupId)
                    .setMemberId(memberId == null ? "" : memberId)
                    .setSessionTimeoutMs(sessionTimeoutMs)
                    .setRebalanceTimeoutMs(rebalanceTimeoutMs)
                    .addAllProtocols(protocols)
                    .build();

            JoinGroupResponse response;
            try {
                response = stub.joinGroup(request);
            } catch (Exception e) {
                sleep(backoffMs);
                backoffMs = Math.min(5_000, backoffMs * 2);
                continue;
            }

            switch (response.getStatus()) {
                case GROUP_OK:
                    return response;

                case MEMBER_ID_REQUIRED:
                    memberId = response.getMemberId();
                    continue;

                case GROUP_REBALANCING:
                case GROUP_LOADING:
                case GROUP_COORDINATOR_CHANGED:
                    sleep(backoffMs);
                    backoffMs = Math.min(5_000, backoffMs * 2);
                    continue;

                case INCOMPATIBLE_PROTOCOL:
                case INVALID_SESSION_TIMEOUT:
                case GROUP_AUTH_FAILED:
                    throw new IllegalStateException("JoinGroup hard fail: " + response.getStatus());
                default:
                    sleep(backoffMs);
                    backoffMs = Math.min(5_000, backoffMs * 2);
            }
        }
    }

    public void syncGroup() {

    }


    public void startHeartBeat() {

    }

    private static void sleep(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
    }
}
