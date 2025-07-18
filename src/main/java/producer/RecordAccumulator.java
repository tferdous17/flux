package producer;

import org.tinylog.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RecordAccumulator {
    static private final int DEFAULT_BATCH_SIZE = 10_240; // 10 KB

    private final int batchSize;
    private Map<Integer, RecordBatch> partitionBatches; // Per-partition batches
    private final int numPartitions = 3; // Default number of partitions, should match broker
    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    public RecordAccumulator() {
        this.batchSize = validateBatchSize(DEFAULT_BATCH_SIZE);
        this.partitionBatches = new HashMap<>();
    }

    public RecordAccumulator(int batchSize) {
        this.batchSize = validateBatchSize(batchSize);
        this.partitionBatches = new HashMap<>();
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        return new RecordBatch(batchSize);
    }

    //TODO: Broker class missing
    public boolean flush() {
        Logger.info("Flushing the batch to the broker (Stubbed out)");
        return true;
    }

    /**
     * Extract partition information from a serialized ProducerRecord
     */
    private int extractPartitionFromRecord(byte[] serializedRecord) {
        try {
            // Deserialize to get the ProducerRecord and extract partition info
            ProducerRecord<String, String> record = ProducerRecordCodec.deserialize(
                    serializedRecord, String.class, String.class);
            
            // If the record has a specific partition set, use it
            // Otherwise, we'll let the broker handle partition selection
            Integer partitionNumber = record.getPartitionNumber();
            if (partitionNumber != null) {
                return partitionNumber % numPartitions; // Ensure partition is within bounds
            }
            
            // For records with a key, use MurmurHash2 for consistent hashing
            String key = record.getKey();
            if (key != null && !key.isEmpty()) {
                return MurmurHash2.selectPartition(key, numPartitions);
            } else {
                // Round-robin for keyless records
                return roundRobinCounter.getAndIncrement() % numPartitions;
            }
        } catch (Exception e) {
            Logger.warn("Failed to extract partition from record, using round-robin: " + e.getMessage());
            return roundRobinCounter.getAndIncrement() % numPartitions;
        }
    }

    /*
    1. Check for both cases:
        1A) First-time batch exists -> currentBatch == null
        1B) Full batches -> !currentBatch.append(record)
    2. If we are NON-first-time batch, and it's full... we should flush and create a new batch since we flushed the old ones.
    3. Therefore, in both cases 1A and 1B we still need to call createBatch()... call createBatch() once only for both cases.
    4. If after logic, we still have a case where the batch is full... investigate further, return failure for now.
    */
    public void append(byte[] serializedRecord) throws IOException {
        // Extract partition from the serialized record
        int partition = extractPartitionFromRecord(serializedRecord);
        RecordBatch currentBatch = partitionBatches.get(partition);
        
        int baseOffset = 0; // TODO: Should be determined by broker/partition
        
        try {
            if (currentBatch == null || !currentBatch.append(serializedRecord)) {
                if (currentBatch != null) { // Case 1B - batch is full
                    Logger.info("Batch for partition " + partition + " is full. Flushing current batch.");
                    flush(); // TODO: Missing implementation
                }
                Logger.info("Creating a new batch for partition " + partition + ".");
                currentBatch = createBatch(partition, baseOffset);
                partitionBatches.put(partition, currentBatch);
                
                if (!currentBatch.append(serializedRecord)) {
                    throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
                }
            }
            Logger.info("Record appended successfully to partition " + partition + ".");
        } catch (Exception e) {
            Logger.error("Failed to append record: " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Get the current batch for a specific partition
     */
    public RecordBatch getCurrentBatch(int partition) {
        return partitionBatches.get(partition);
    }

    /**
     * Get the current batch for partition 0 (backward compatibility)
     */
    public RecordBatch getCurrentBatch() {
        return getCurrentBatch(0);
    }

    /**
     * Get all partition batches
     */
    public Map<Integer, RecordBatch> getPartitionBatches() {
        return new HashMap<>(partitionBatches); // Return copy to prevent external modification
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void printRecord() {
        Logger.info("Batch Size: " + getBatchSize());
        Logger.info("Partition Batches:");
        
        partitionBatches.forEach((partition, batch) -> {
            Logger.info("Partition " + partition + ":");
            batch.printBatchDetails();
        });
    }

    private int validateBatchSize(int batchSize) {
        final int MIN_BATCH_SIZE = 1; // Minimum size
        final int MAX_BATCH_SIZE = 1_048_576; // 1 MB

        if (batchSize < MIN_BATCH_SIZE || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    "Batch size must be between " + MIN_BATCH_SIZE + "-" + MAX_BATCH_SIZE + " bytes."
            );
        }
        return batchSize;
    }
}
