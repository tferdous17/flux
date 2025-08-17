package server.internal;

import commons.FluxTopic;
import metadata.InMemoryTopicMetadataRepository;
import metadata.Metadata;
import org.tinylog.Logger;
import producer.IntermediaryRecord;
import producer.RecordBatch;
import proto.Message;
import server.internal.storage.Partition;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions;
    private List<Partition> partitions;
    private int partitionIdCounter = 1;
    private AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final PartitionWriteManager writeManager;

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
            this.partitions.add(new Partition("default-topic", partitionIdCounter++));
        }
    }

    public Broker(String brokerId, String host, int port) throws IOException {
        this(brokerId, host, port, 1); // Default to 3 partitions
    }

    public Broker() throws IOException {
        // Default to 1 partition since all records technically require a topic field.
        this("BROKER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()), "localhost", 50051, 1);
    }

    public void createTopics(Collection<proto.Topic> topics) throws IOException {
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
}
