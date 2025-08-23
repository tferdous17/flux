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
import io.grpc.ManagedChannelRegistry;
import metadata.InMemoryTopicMetadataRepository;
import metadata.Metadata;
import org.tinylog.Logger;
import producer.IntermediaryRecord;
import producer.RecordBatch;
import proto.*;
import server.internal.storage.Partition;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker implements Controller {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions;
    private List<Partition> partitions;
    private int partitionIdCounter = 1;
    private AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final PartitionWriteManager writeManager;

    private boolean isActiveController = false;
    private String controllerEndpoint = ""; // "localhost:50051"
    private Map<String, String> followerNodeEndpoints = Collections.synchronizedMap(new HashMap<>()); // <broker id, broker address>
    private ControllerServiceGrpc.ControllerServiceFutureStub futureStub;
    private ManagedChannel channel;

    private static final int MAX_REPLICATION_FACTOR = 3;

    public Broker(String brokerId, String host, int port, int numPartitions) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.numPartitions = numPartitions;
        this.partitions = new ArrayList<>();
        this.writeManager = new PartitionWriteManager();

        // Create multiple partitions
        for (int i = 0; i < numPartitions; i++) {
            // All the default partitions that get created upon broker initialization (not part of any topic) will have this topic name
            this.partitions.add(new Partition("DEFAULT", partitionIdCounter++));
        }
    }

    public Broker(String brokerId, String host, int port) throws IOException {
        this(brokerId, host, port, 1); // Default to 1 partitions
    }

    public Broker() throws IOException {
        // Default to 1 partition since all records technically require a topic field.
        this("BROKER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()), "localhost", 50051, 1);
    }

    @Override
    public void createTopics(Collection<proto.Topic> topics) throws IOException {
        if (!isActiveController) {
            return;
        }

        // right now just worry about creating 1 topic
        proto.Topic firstTopic = topics.stream().findFirst()
                .orElseThrow(() -> new IllegalArgumentException("topics cannot be empty"));
        String topicName = firstTopic.getTopicName();
        int numPartitionsToCreate = firstTopic.getNumPartitions();
        int replicationFactor = firstTopic.getReplicationFactor();

        // Will throw runtime exception if it can not validate this creation request
        validateTopicCreation(topicName, numPartitions, replicationFactor);

        List<Partition> topicPartitions = new ArrayList<>();
        for (int i = 0; i < numPartitionsToCreate; i++) {
            Partition p = new Partition(topicName, partitionIdCounter++);
            this.partitions.add(p);
            topicPartitions.add(p);
            this.numPartitions++;
        }

        FluxTopic topic = new FluxTopic(topicName, topicPartitions, replicationFactor);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, topic);
        Logger.info("BROKER: Create topics completed successfully.");
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

                    // TODO: Broker must now gracefully shutdown upon successful ack from controller
                }

                @Override
                public void onFailure(Throwable t) {
                    Logger.error(t);
                }
            }, FluxExecutor.getExecutorService());
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

    public int produceSingleMessage(int targetPartitionId, byte[] record) throws IOException {
        // Note: Partition IDs are NOT 0-indexed
        Partition targetPartition = partitions.get(targetPartitionId - 1);

        // Use the write manager for thread-safe write operation
        return writeManager.writeToPartition(targetPartition, record);
    }

    // ! Not currently using this below method, general functionality already handled by the other produceMessages()
    public void produceMessages(RecordBatch batch) throws IOException {
        // For batches, use round-robin distribution since we can't easily extract keys
        // from the batch without decomposing it
        int targetPartitionId = roundRobinCounter.getAndIncrement() % numPartitions;
        Partition targetPartition = partitions.get(targetPartitionId);

        // Use the write manager for thread-safe batch write operation
        writeManager.writeRecordBatchToPartition(targetPartition, batch);
        Logger.info("Appended record batch to broker partition %d".formatted(targetPartitionId));
    }

    public int produceMessages(List<IntermediaryRecord> messages) throws IOException {
        // we can just call the produceSingleMessage() for each byte[] in messages
        int counter = 0;
        int lastRecordOffset = -1;
        for (IntermediaryRecord record : messages) {
            lastRecordOffset = produceSingleMessage(record.targetPartition(), record.data());
            counter++;
        }
        Logger.info("Appended %d records to broker.".formatted(counter));
        System.out.println("PRINTING # OF RECORDS PER PARTITION:");
        for (Partition partition : partitions) {
            System.out.println("Partition " + partition.getPartitionId() + " contains: " + partition.getCurrentOffset() + " records");
        }

        return lastRecordOffset;
    }

    // TODO: Finish consumer infrastructure
    public Message consumeMessage(int startingOffset) throws IOException {
        // Default to partition 0 for backward compatibility
        // TODO: Replace placeholder partitionID
        return consumeMessage(1, startingOffset);
    }

    /**
     * Consume a message from a specific partition at the given offset
     */
    public Message consumeMessage(int partitionId, int startingOffset) throws IOException {
        if (partitionId < 1 || partitionId > numPartitions) {
            throw new IllegalArgumentException(
                    "Invalid partition ID: %d. Valid range: 1-%d".formatted(partitionId, numPartitions));
        }

        Partition targetPartition = partitions.get(partitionId - 1);
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
     * Get a specific partition by ID
     */
    public Partition getPartition(int partitionId) {
        if (partitionId < 1 || partitionId > numPartitions) {
            throw new IllegalArgumentException(
                    "Invalid partition ID: %d. Valid range: 1-%d".formatted(partitionId, numPartitions));
        }
        return partitions.get(partitionId - 1);
    }

    /**
     * Get all partitions
     */
    public List<Partition> getPartitions() {
        return new ArrayList<>(partitions); // Return a copy to prevent external modification
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

    public Map<String, String> getFollowerNodeEndpoints() {
        return followerNodeEndpoints;
    }
}
