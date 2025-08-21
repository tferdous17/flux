package consumer;

import commons.FluxExecutor;
import commons.headers.Headers;
import consumer.assignors.PartitionAssignor;
import consumer.assignors.RangeAssignor;
import consumer.assignors.RoundRobinAssignor;
import consumer.assignors.StickyAssignor;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import org.tinylog.Logger;
import proto.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class FluxConsumer<K, V> implements Consumer {
    private final ConsumerServiceGrpc.ConsumerServiceBlockingStub blockingStub;
    private final GroupCoordinatorServiceGrpc.GroupCoordinatorServiceBlockingStub groupCoordinatorServiceBlockingStub;
    private ManagedChannel channel;
    private GroupCoordinator groupCoordinatorClient;

    private int currentOffset = 0;
    private String groupId = "my-group";
    private String memberId = "";
    private int generationId = -1;
    private String leaderId = "";
    private boolean isLeader = false;
    private Assignment myAssignment;

    private Collection<String>  subscribedTopics = List.of();
    private final Map<String, List<Integer>> assignedTopicPartitions = new ConcurrentHashMap<>();
    private volatile Assignment assignment;
    private ScheduledExecutorService hbExec;
    private ScheduledFuture<?> hbTask;

    private int sessionTimeoutsMs = 10_000;
    private int rebalanceTimoutMs = 30_000;


    public FluxConsumer() {
        channel = Grpc.newChannelBuilder("localhost:50051", InsecureChannelCredentials.create()).build();
        blockingStub = ConsumerServiceGrpc.newBlockingStub(channel);
        groupCoordinatorServiceBlockingStub = GroupCoordinatorServiceGrpc.newBlockingStub(channel);
    }

    @Override
    public void subscribe(Collection<String> topics) {
        this.subscribedTopics = new ArrayList<>(topics);

        String rack = "us-east"; // TODO: Lets keep it EAST for now lol

        List<ProtocolMetadata> protocols = List.of(
                ProtocolCodec.buildProtocolMetadata(this.subscribedTopics, "range", rack),
                ProtocolCodec.buildProtocolMetadata(this.subscribedTopics, "roundrobin", rack)
        );

        JoinGroupResponse joinResponse = groupCoordinatorClient.joinGroupLoop(
                groupId,
                (memberId == null ? "": memberId),
                sessionTimeoutsMs,
                rebalanceTimoutMs,
                protocols
        );

        this.memberId = joinResponse.getMemberId();
        this.generationId = joinResponse.getGenerationId();
        this.leaderId = joinResponse.getLeaderId();
        this.isLeader = this.memberId.equals(this.leaderId);

        // CASE 1: Build FULL assignment => send all followers info with SyncGroup.
        if (isLeader) {
            // Populate members.
            List<String> memberIds = new ArrayList<>();
            if (joinResponse.getMembersCount() > 0){
                for (MemberInfo info: joinResponse.getMembersList()) {
                    memberIds.add(info.getMemberId());
                }
            }

            // EDGE CASE: we missed ourselves from the total list.
            if (!memberIds.contains(memberId)) memberIds.add(memberId);

            // Choose assignor based on negotiated protocol
            PartitionAssignor assignor = selectAssignor(joinResponse.getProtocol());
            Map<String, Integer> topicToPartitionCount = fetchPartitionCounts(subscribedTopics);

            Map<String, Map<String, List<Integer>>> full =  assignor.assign(memberIds, topicToPartitionCount);

            LeaderAssignmentPlanner planner = new LeaderAssignmentPlanner(assignor);
            Assignment groupBlob = planner.buildGroupAssignment(memberIds, subscribedTopics, fetchPartitionCounts(subscribedTopics));

            // Leader sends the full assignment
            SyncGroupResponse sync = groupCoordinatorServiceBlockingStub.syncGroup( // TODO: Missing SyncGroup.
                    SyncGroupRequest.newBuilder()
                            .setGroupId(groupId)
                            .setGenerationId(generationId)
                            .setMemberId(memberId)
                            .setAssignment(groupBlob)
                            .build()
            );

            // Server returns my member-specific slice
            this.myAssignment = sync.getAssignment();
        }
        else { // CASE 2: We are a Follower, get own assignment.
            SyncGroupResponse sync = groupCoordinatorServiceBlockingStub.syncGroup( // TODO: Missing SyncGroup.
                    SyncGroupRequest.newBuilder()
                            .setGroupId(groupId)
                            .setGenerationId(generationId)
                            .setMemberId(memberId)
                            .setAssignment(Assignment.getDefaultInstance())
                            .build()
            );
            this.myAssignment = sync.getAssignment();
        }

        // TODO: Install my assignment for poll()
        Map<String, List<Integer>> tp = ProtocolCodec.unpackAssignment(this.myAssignment);
        installAssignment(tp);

        // TODO: Start heartbeats
        groupCoordinatorClient.startHeartBeat();
    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public PollResult poll(Duration timeout) {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        FetchMessageRequest req = FetchMessageRequest
                .newBuilder()
                .setStartingOffset(currentOffset)
                .build();

        try {
            // push fetch message execution into worker thread
            Future<FetchMessageResponse> future = FluxExecutor.getExecutorService().submit(() -> blockingStub.fetchMessage(req));
            // waits for task to complete for at most the given timeout
            FetchMessageResponse response = future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            if (response.getStatus().equals(Status.READ_COMPLETION)) {
                Logger.info("READ COMPLETION");
                return new PollResult(records, false);
            }

            Message msg = response.getMessage();
            ConsumerRecord<String, String> record = new ConsumerRecord<>(
                    msg.getTopic(),
                    msg.getPartition(),
                    msg.getOffset(),
                    msg.getTimestamp(),
                    msg.getKey(),
                    msg.getValue(),
                    new Headers()
            );
            records.add(record);
            currentOffset = msg.getOffset() + 1;

        } catch (TimeoutException e) {
            // if no data fetched before timeout limit
            Logger.info("Timeout waiting for message");
            return new PollResult(records, true);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            System.err.println("Fetch failed: " + e.getCause().getMessage());
            return new PollResult(records, false);
        }

        return new PollResult(records, true);
    }

    @Override
    public void commitOffsets() {

    }

    private PartitionAssignor selectAssignor(String protocol) {
        if (protocol == null) protocol = "range";
        return switch (protocol.toLowerCase()) {
            case "roundrobin" -> new RoundRobinAssignor();
            case "sticky" -> new StickyAssignor();
            default -> new RangeAssignor();
        };
    }

    // TODO: Replace with a real metadata RPC (e.g., MetadataService) or cache.
    private Map<String, Integer> fetchPartitionCounts(Collection<String> topics) {
        Map<String, Integer> counts = new java.util.LinkedHashMap<>();
        for (String t : topics) {
            if (t != null && !t.isEmpty()) {
                counts.put(t, 3); // TODO: real value from broker; 3 is a safe mock
            }
        }
        return counts;
    }

    private void installAssignment(Map<String, List<Integer>> tp) {
        assignedTopicPartitions.clear();
        if (tp != null) {
            for (Map.Entry<String, List<Integer>> e : tp.entrySet()) {
                // copy to avoid external mutation
                assignedTopicPartitions.put(e.getKey(), new ArrayList<>(e.getValue()));
            }
        }
        Logger.info("Installed assignment: " + assignedTopicPartitions);
        // TODO: If fetch API needs it, build/refresh a local fetch plan from this map.
    }

}
