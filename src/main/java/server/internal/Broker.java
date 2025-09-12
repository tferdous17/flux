package server.internal;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import commons.FluxExecutor;
import commons.FluxTopic;
import commons.utils.PartitionWriteManager;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import metadata.InMemoryTopicMetadataRepository;
import metadata.Metadata;
import metadata.snapshots.BrokerMetadata;
import metadata.snapshots.PartitionMetadata;
import org.tinylog.Logger;
import producer.IntermediaryRecord;
import producer.RecordBatch;
import proto.*;
import server.internal.storage.Partition;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class Broker implements Controller {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions;
    private Map<String, List<Partition>> topicPartitions; // Map of topic name to its partitions
    private AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final PartitionWriteManager writeManager;

    private boolean isActiveController = false;
    private String clusterId = ""; // that this controller belongs to
    private String controllerEndpoint = ""; // "localhost:50051"
    private Map<String, String> followerNodeEndpoints = Collections.synchronizedMap(new HashMap<>()); // <broker id, broker address>
    private AtomicReference<BrokerMetadata> controllerMetadata = new AtomicReference<>();
    private Map<String, BrokerMetadata> cachedFollowerMetadata = Collections.synchronizedMap(new HashMap<>()); // <broker id, broker metadata obj>
    private ControllerServiceGrpc.ControllerServiceFutureStub futureStub;
    private ManagedChannel channel;
    private ShutdownCallback shutdownCallback;
    private AtomicInteger followerRoundRobinCounter = new AtomicInteger(0);

    private static final int MAX_REPLICATION_FACTOR = 3;

    public Broker(String brokerId, String host, int port, int numPartitions) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.numPartitions = 0; // Start with 0 partitions, they'll be created with topics
        this.topicPartitions = new ConcurrentHashMap<>();
        this.writeManager = new PartitionWriteManager();

        FluxExecutor
                .getSchedulerService()
                .scheduleWithFixedDelay(this::updateBrokerMetadata, 80, 180, TimeUnit.SECONDS);
    }

    public Broker(String brokerId, String host, int port) throws IOException {
        this(brokerId, host, port, 0); // Start with no partitions
    }

    public Broker() throws IOException {
        // Start with no partitions - they'll be created with topics
        this("BROKER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()), "localhost", 50051, 0);
    }

    @Override
    public void createTopics(Collection<proto.Topic> topics) throws IOException {
        if (!isActiveController) {
            return;
        }

        // For each topic to create, we must create a Broker to Partition mapping
        // Where the mapping represents which partitions should be created under which brokers
        // So instead of creating every topic's partitions on a single broker, we distribute them evenly via roundrobin
        // So each broker will get their turn at a partition, then the counter will continue on until we created
        // all the requested partitions for that topic

        // How to create the mapping itself? Mapping should represent the broker that should receive the partition(s)
        // and the IDs of the partition itself.
        // Upon receiving the mappings, the brokers themselves should create and store the partitions themselves
        List<String> followers = new ArrayList<>(followerNodeEndpoints.keySet());
        Map<String, List<BrokerToPartitionMapping>> followerMaps = new HashMap<>();

        for (proto.Topic topic : topics) {
            // Validate topic first
            String topicName = topic.getTopicName();
            int numPartitionsToCreate = topic.getNumPartitions();
            int replicationFactor = topic.getReplicationFactor();

            // Will throw runtime exception if it can not validate this creation request
            validateTopicCreation(topicName, numPartitionsToCreate, replicationFactor);

            Map<String, List<Integer>> partitionIds = new HashMap<>();
            int n = topic.getNumPartitions();
            for (int i = 0; i < n; i++) {

                // Get the broker that we need this partition to go to
                int brokerIndex = followerRoundRobinCounter.getAndIncrement() % followerNodeEndpoints.size();
                String brokerId = followers.get(brokerIndex);
                String brokerAddr = followerNodeEndpoints.get(brokerId);

                if (partitionIds.containsKey(brokerAddr)) {
                    partitionIds.get(brokerAddr).add(brokerIndex);
                } else {
                    partitionIds.put(brokerAddr, new ArrayList<>());
                    partitionIds.get(brokerAddr).add(brokerIndex);
                }
            }

            // After creating all the partitionIds, create a mapping for each broker addr
            followerNodeEndpoints.forEach((id, addr) -> {
                BrokerToPartitionMapping map = new BrokerToPartitionMapping(addr, topic.getTopicName(), partitionIds.get(addr));
                followerMaps.get(addr).add(map);
            });

        }

        // After all mappings created, then send out a request to each follower
        // Each request will contain all the partitions it needs to create per topic
        // Ex: Map1{ "localhost:50051", "topic1", [1,5] }, Map2{ "localhost:50051", "topic2", [0,4] }
        // Since topics have their own unique set of ids separate from other topics, gotta ensure partitions dont conflict in any way

        // right now just worry about creating 1 topic
        proto.Topic firstTopic = topics.stream().findFirst()
                .orElseThrow(() -> new IllegalArgumentException("topics cannot be empty"));
        String topicName = firstTopic.getTopicName();
        int numPartitionsToCreate = firstTopic.getNumPartitions();
        int replicationFactor = firstTopic.getReplicationFactor();

        // Will throw runtime exception if it can not validate this creation request
        validateTopicCreation(topicName, numPartitionsToCreate, replicationFactor);

        List<Partition> newTopicPartitions = new ArrayList<>();
        // Each topic's partitions start from ID 0
        for (int i = 0; i < numPartitionsToCreate; i++) {
            Partition p = new Partition(topicName, i);
            newTopicPartitions.add(p);
            this.numPartitions++;
        }
        this.topicPartitions.put(topicName, newTopicPartitions);

        FluxTopic topic = new FluxTopic(topicName, newTopicPartitions, replicationFactor);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, topic);
        Logger.info("BROKER: Create topics completed successfully.");
    }

    public synchronized void createAssignedTopicPartitions(List<BrokerToPartitionMapping> mappings) {
        // Ex: Map1{ "localhost:50051", "topic1", [1,5] }, Map2{ "localhost:50051", "topic2", [0,4] }
        for (BrokerToPartitionMapping mapping : mappings) {
            List<Integer> partitionIds = mapping.partitionIds();

            for (Integer i : partitionIds) {
                try {
                    Partition p = new Partition(mapping.topicName(), i);
                    topicPartitions.computeIfAbsent(mapping.topicName(), k -> new ArrayList<>()).add(p);
                    numPartitions++;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public void registerBroker() {
        // Allows non-controller nodes to send registration requests to the current active controller in the cluster
        if (!isActiveController && !controllerEndpoint.isEmpty()) {
            // Establish channel connection first to the controller + stub instantiation
            channel = Grpc.newChannelBuilder(controllerEndpoint, InsecureChannelCredentials.create()).build();
            futureStub = ControllerServiceGrpc.newFutureStub(channel);

            BrokerRegistrationRequest request = BrokerRegistrationRequest
                    .newBuilder()
                    .setBrokerId(this.brokerId)
                    .setBrokerHost(this.host)
                    .setBrokerPort(this.port)
                    .build();

            Logger.info(brokerId + " @ " + host + ":" + port + " SENDING OVER REGISTER BROKER REQUEST TO CONTROLLER @ " + controllerEndpoint);
            ListenableFuture<BrokerRegistrationResult> response = futureStub.registerBroker(request);
            Futures.addCallback(response, new FutureCallback<BrokerRegistrationResult>() {
                @Override
                public void onSuccess(BrokerRegistrationResult result) {
                    Logger.info(result.getAcknowledgement());
                }

                @Override
                public void onFailure(Throwable t) {
                    Logger.error(t);
                }
            }, FluxExecutor.getExecutorService());
        }
    }

    @Override
    public void decommissionBroker() {
        if (controllerEndpoint.isEmpty()) {
            Logger.warn("Cannot decommission broker that is not part of any cluster.");
            return;
        }

        if (!isActiveController) {
            DecommissionBrokerRequest request = DecommissionBrokerRequest
                    .newBuilder()
                    .setBrokerId(this.brokerId)
                    .build();

            Logger.info(brokerId + " @ " + host + ":" + port + " SENDING OVER DECOMMISSION BROKER REQUEST TO CONTROLLER @ " + controllerEndpoint);
            ListenableFuture<DecommissionBrokerResult> response = futureStub.decommissionBroker(request);
            Futures.addCallback(response, new FutureCallback<DecommissionBrokerResult>() {
                @Override
                public void onSuccess(DecommissionBrokerResult result) {
                    Logger.info(result.getAcknowledgement());
                    try {
                        shutdownCallback.stop();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    Logger.error(t);
                }
            }, FluxExecutor.getExecutorService());
        }
    }

    public void registerShutdownCallback(ShutdownCallback callback) {
        this.shutdownCallback = callback;
    }

    // ! Only for testing purposes
    public void triggerManualBrokerShutdown() {
        try {
            Logger.info("Triggering manual broker shutdown.");
            shutdownCallback.stop();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    // Initializes the controller's metadata before other brokers in the cluster
    public void initControllerBrokerMetadata() {
        if (!isActiveController) {
            return;
        }

        Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<>();
        topicPartitions.forEach((_, partitionList) -> {
            partitionList.forEach(p -> {
                partitionMetadataMap.put(
                        p.getPartitionId(),
                        new PartitionMetadata(p.getPartitionId(), this.brokerId)
                );
            });
        });

        this.controllerMetadata.set(new BrokerMetadata(
                this.brokerId,
                this.host,
                this.port,
                this.numPartitions,
                partitionMetadataMap
        ));
    }

    public void updateBrokerMetadata() {
        // Periodically the broker will send its most up-to-date metadata to the Controller node
        Map<Integer, PartitionMetadata> partitionMetadataMap = new HashMap<>();
        topicPartitions.forEach((_, partitionList) -> {
            partitionList.forEach(p -> {
                partitionMetadataMap.put(
                        p.getPartitionId(),
                        new PartitionMetadata(p.getPartitionId(), this.brokerId)
                );
            });
        });

        if (!isActiveController) {
            UpdateBrokerMetadataRequest request = UpdateBrokerMetadataRequest
                    .newBuilder()
                    .setBrokerId(this.brokerId)
                    .setHost(this.host)
                    .setPortNumber(this.port)
                    .setNumPartitions(this.numPartitions)
                    .putAllPartitionDetails(PartitionMetadata.toDetailsMapProto(partitionMetadataMap))
                    .build();

            // Send off request to controller and await response
            ListenableFuture<UpdateBrokerMetadataResponse> response = futureStub.updateBrokerMetadata(request);
            Futures.addCallback(response, new FutureCallback<UpdateBrokerMetadataResponse>() {

                @Override
                public void onSuccess(UpdateBrokerMetadataResponse result) {
                   Logger.info(result.getAcknowledgement());
                }

                @Override
                public void onFailure(Throwable t) {
                    Logger.error(t);
                }
            }, FluxExecutor.getExecutorService());
        } else {
            // Keep controller's metadata up-to-date as well
            this.controllerMetadata.set(new BrokerMetadata(
                    this.brokerId,
                    this.host,
                    this.port,
                    this.numPartitions,
                    partitionMetadataMap
            ));
        }
    }

    @Override
    public void processBrokerHeartbeat() {
        if (!isActiveController) {
            return;
        }
    }

    private void validateTopicCreation(String topicName, int numPartitions, int replicationFactor) {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Cannot create topic with empty name.");
        }
        if (InMemoryTopicMetadataRepository.getInstance().getActiveTopics().contains(topicName)) {
            throw new IllegalArgumentException("Topic already exists: %s".formatted(topicName));
        }
        if (numPartitions < 1) {
            throw new IllegalArgumentException("Number of partitions can not be less than 1: %d".formatted(numPartitions));
        }
        if (replicationFactor < 0 || replicationFactor > MAX_REPLICATION_FACTOR) {
            throw new IllegalArgumentException("Replication factor can not be negative or >%d: %d".formatted(MAX_REPLICATION_FACTOR, replicationFactor));
        }
    }

    /**
     * Helper method to get a partition by topic and partition ID
     */
    private Partition getPartitionForTopic(String topicName, int partitionId) {
        List<Partition> partitions = topicPartitions.get(topicName);
        if (partitions == null) {
            throw new IllegalArgumentException("Topic does not exist: " + topicName);
        }
        if (partitionId < 0 || partitionId >= partitions.size()) {
            throw new IllegalArgumentException(
                "Invalid partition ID %d for topic %s. Valid range: 0-%d"
                    .formatted(partitionId, topicName, partitions.size() - 1));
        }
        return partitions.get(partitionId);
    }

    public int produceSingleMessage(String topicName, int targetPartitionId, byte[] record) throws IOException {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name is required");
        }
        // Note: Partition IDs are 0-indexed now
        Partition targetPartition = getPartitionForTopic(topicName, targetPartitionId);

        // Use the write manager for thread-safe write operation
        return writeManager.writeToPartition(targetPartition, record);
    }

    // ! Not currently using this below method, general functionality already handled by the other produceMessages()
    public void produceMessages(String topicName, RecordBatch batch) throws IOException {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name is required");
        }
        List<Partition> partitions = topicPartitions.get(topicName);
        if (partitions == null || partitions.isEmpty()) {
            throw new IllegalArgumentException("Topic does not exist or has no partitions: " + topicName);
        }
        // For batches, use round-robin distribution since we can't easily extract keys
        // from the batch without decomposing it
        int targetPartitionId = roundRobinCounter.getAndIncrement() % partitions.size();
        Partition targetPartition = partitions.get(targetPartitionId);

        // Use the write manager for thread-safe batch write operation
        writeManager.writeRecordBatchToPartition(targetPartition, batch);
        Logger.info("Appended record batch to broker partition %d of topic %s".formatted(targetPartitionId, topicName));
    }

    public int produceMessages(List<IntermediaryRecord> messages) throws IOException {
        // we can just call the produceSingleMessage() for each byte[] in messages
        int counter = 0;
        int lastRecordOffset = -1;
        for (IntermediaryRecord record : messages) {
            lastRecordOffset = produceSingleMessage(record.topicName(), record.targetPartition(), record.data());
            counter++;
        }
        Logger.info("Appended %d records to broker.".formatted(counter));
        System.out.println("PRINTING # OF RECORDS PER PARTITION:");
        for (Map.Entry<String, List<Partition>> entry : topicPartitions.entrySet()) {
            String topic = entry.getKey();
            for (Partition partition : entry.getValue()) {
                System.out.println("Topic " + topic + " - Partition " + partition.getPartitionId() + " contains: " + partition.getCurrentOffset() + " records");
            }
        }

        return lastRecordOffset;
    }

    // TODO: Finish consumer infrastructure
    public Message consumeMessage(int startingOffset) throws IOException {
        // Default to partition 0 for backward compatibility
        // For now, assume the first available topic since consumer context isn't fully implemented
        if (topicPartitions.isEmpty()) {
            throw new IllegalStateException("No topics available - consumer must subscribe to a topic first");
        }

        String firstTopicName = topicPartitions.keySet().iterator().next();
        // TODO: Replace placeholder partitionID - using partition 0 as default
        return consumeMessage(firstTopicName, 0, startingOffset);
    }

    /**
     * Consume a message from a specific topic partition at the given offset
     */
    public Message consumeMessage(String topicName, int partitionId, int startingOffset) throws IOException {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name is required");
        }
        Partition targetPartition = getPartitionForTopic(topicName, partitionId);
        return targetPartition.getRecordAtOffset(startingOffset);
    }

    public String getBrokerId() {
        return brokerId;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Get a specific partition by topic and ID
     */
    public Partition getPartition(String topicName, int partitionId) {
        if (topicName == null || topicName.isEmpty()) {
            throw new IllegalArgumentException("Topic name is required");
        }
        return getPartitionForTopic(topicName, partitionId);
    }

    /**
     * Get all partitions across all topics
     */
    public List<Partition> getPartitions() {
        List<Partition> allPartitions = new ArrayList<>();
        for (List<Partition> partitionList : topicPartitions.values()) {
            allPartitions.addAll(partitionList);
        }
        return allPartitions; // Return a copy to prevent external modification
    }

    /**
     * Get partitions for a specific topic
     */
    public List<Partition> getPartitionsForTopic(String topicName) {
        List<Partition> partitions = topicPartitions.get(topicName);
        if (partitions == null) {
            return Collections.emptyList();
        }
        return new ArrayList<>(partitions); // Return a copy
    }

    /**
     * Get the count of partitions (alias for getNumPartitions for clarity)
     */
    public int getPartitionCount() {
        return numPartitions;
    }

    public void setIsActiveController(boolean isActiveController) {
        this.isActiveController = isActiveController;
    }

    public boolean isActiveController() {
        return this.isActiveController;
    }

    public void setControllerEndpoint(String controllerEndpoint) {
        this.controllerEndpoint = controllerEndpoint;
    }

    public String getControllerEndpoint() {
        return this.controllerEndpoint;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public Map<String, String> getFollowerNodeEndpoints() {
        return followerNodeEndpoints;
    }

    public Map<String, BrokerMetadata> getCachedFollowerMetadata() {
        return cachedFollowerMetadata;
    }

    public AtomicReference<BrokerMetadata> getControllerMetadata() {
        return controllerMetadata;
    }
}
