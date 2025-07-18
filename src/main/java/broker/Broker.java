package broker;

import org.tinylog.Logger;
import producer.RecordBatch;
import producer.ProducerRecord;
import producer.ProducerRecordCodec;
import producer.MurmurHash2;
import proto.Message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Broker {
    private String brokerId;
    private String host;
    private int port; // ex: port 8080
    private int numPartitions;
    private List<Partition> partitions;
    private AtomicInteger roundRobinCounter = new AtomicInteger(0);
    private int nextAvailOffset; // record offsets

    public Broker(String brokerId, String host, int port, int numPartitions) throws IOException {
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
        this.numPartitions = numPartitions;
        this.partitions = new ArrayList<>();
        
        // Create multiple partitions
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new Partition(i));
        }
        this.nextAvailOffset = 0;
    }

    public Broker(String brokerId, String host, int port) throws IOException {
        this(brokerId, host, port, 3); // Default to 3 partitions
    }

    public Broker() throws IOException {
        this("BROKER-1", "localhost", 50051, 3); // Default to 3 partitions
    }

    /**
     * Validates that the partition number is within valid range.
     * Partition selection should be done by the producer, not the broker.
     */
    private void validatePartition(int partitionId) {
        if (partitionId < 0 || partitionId >= numPartitions) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId + 
                    ". Valid range: 0-" + (numPartitions - 1));
        }
    }

    public int produceSingleMessage(byte[] record) throws IOException {
        // Deserialize the record to extract key for partition selection
        ProducerRecord<String, String> prodRecord = ProducerRecordCodec.deserialize(
                record, String.class, String.class);
        
        // Use the partition already selected by the producer
        int targetPartitionId = prodRecord.getPartitionNumber() != null ? prodRecord.getPartitionNumber() : 0; // fallback
                                                                                                               // to
                                                                                                               // partition
                                                                                                               // 0
        
        validatePartition(targetPartitionId);
        Partition targetPartition = partitions.get(targetPartitionId);
        
        // Update record offset in header (first 4 bytes)
        ByteBuffer buffer = ByteBuffer.wrap(record);
        buffer.putInt(0, nextAvailOffset);

        Logger.info("PRODUCE SINGLE MESSAGE: Routing to partition " + targetPartitionId + 
                   " with key: " + prodRecord.getKey());
        Logger.info("PRODUCE SINGLE MESSAGE: " + Arrays.toString(buffer.array()));

        int currRecordOffset = nextAvailOffset;
        nextAvailOffset++;

        targetPartition.appendSingleRecord(record, currRecordOffset);
        Logger.info("1. Appended record to broker partition " + targetPartitionId);

        return currRecordOffset;
    }

    // TODO: Replace mock implementation when gRPC is implemented
    public void produceMessages(RecordBatch batch) throws IOException {
        // For batches, use round-robin distribution since we can't easily extract keys
        // from the batch without decomposing it
        int targetPartitionId = roundRobinCounter.getAndIncrement() % numPartitions;
        Partition targetPartition = partitions.get(targetPartitionId);
        
        targetPartition.appendRecordBatch(batch);
        Logger.info("Appended record batch to broker partition " + targetPartitionId);
    }

    public int produceMessages(List<byte[]> messages) throws IOException {
        // we can just call the produceSingleMessage() for each byte[] in messages
        int counter = 0;
        int lastRecordOffset = nextAvailOffset;
        for (byte[] message : messages) {
            lastRecordOffset = produceSingleMessage(message);
            counter++;
        }
        Logger.info("Appended " + counter + " records to broker.");
        return lastRecordOffset;
    }

    // TODO: Finish consumer infrastructure
    public Message consumeMessage(int startingOffset) throws IOException {
        // Default to partition 0 for backward compatibility
        return consumeMessage(0, startingOffset);
    }
    
    /**
     * Consume a message from a specific partition at the given offset
     */
    public Message consumeMessage(int partitionId, int startingOffset) throws IOException {
        if (partitionId < 0 || partitionId >= numPartitions) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId + 
                    ". Valid range: 0-" + (numPartitions - 1));
        }
        
        Partition targetPartition = partitions.get(partitionId);
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
        if (partitionId < 0 || partitionId >= numPartitions) {
            throw new IllegalArgumentException("Invalid partition ID: " + partitionId + 
                    ". Valid range: 0-" + (numPartitions - 1));
        }
        return partitions.get(partitionId);
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
