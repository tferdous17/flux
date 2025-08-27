package server.internal;

import commons.FluxTopic;
import commons.utils.PartitionWriteManager;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions;
    private Map<String, List<Partition>> topicPartitions; // Map of topic name to its partitions
    private AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private final PartitionWriteManager writeManager;

    private static final int MAX_REPLICATION_FACTOR = 3;

    public Broker(String brokerId, String host, int port, int numPartitions) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.numPartitions = 0; // Start with 0 partitions, they'll be created with topics
        this.topicPartitions = new ConcurrentHashMap<>();
        this.writeManager = new PartitionWriteManager();
    }

    public Broker(String brokerId, String host, int port) throws IOException {
        this(brokerId, host, port, 0); // Start with no partitions
    }

    public Broker() throws IOException {
        // Start with no partitions - they'll be created with topics
        this("BROKER-%d".formatted(Metadata.brokerIdCounter.getAndIncrement()), "localhost", 50051, 0);
    }

    public void createTopics(Collection<proto.Topic> topics) throws IOException {
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
}
