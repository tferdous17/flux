package producer;

import commons.utils.PartitionSelector;
import metadata.InMemoryTopicMetadataRepository;
import org.tinylog.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RecordAccumulator {
    static private final int DEFAULT_BATCH_SIZE = 10_240; // 10 KB
    static private final int DEFAULT_MAX_BUFFER_SIZE = 32 * 1024 * 1024; // 32 MB

    private final int batchSize;
    private final int maxBufferSize;
    private volatile long totalBytesUsed; // Track memory usage across all batches
    private Map<Integer, RecordBatch> partitionBatches; // Per-partition batches
    private final int numPartitions;

    public RecordAccumulator(int numPartitions) {
        this.batchSize = validateBatchSize(DEFAULT_BATCH_SIZE);
        this.maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        this.totalBytesUsed = 0;
        this.partitionBatches = new ConcurrentHashMap<>();
        this.numPartitions = numPartitions;
    }

    public RecordAccumulator(int batchSize, int numPartitions) {
        this.batchSize = validateBatchSize(batchSize);
        this.maxBufferSize = DEFAULT_MAX_BUFFER_SIZE;
        this.totalBytesUsed = 0;
        this.partitionBatches = new ConcurrentHashMap<>();
        this.numPartitions = numPartitions;
    }

    public RecordAccumulator(int batchSize, int maxBufferSize, int numPartitions) {
        this.batchSize = validateBatchSize(batchSize);
        this.maxBufferSize = maxBufferSize;
        this.totalBytesUsed = 0;
        this.partitionBatches = new ConcurrentHashMap<>();
        this.numPartitions = numPartitions;
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        return new RecordBatch(batchSize);
    }

    public boolean flush() {
        Logger.info("Flushing the batch to the broker (Stubbed out)");
        return true;
    }

    // TODO: There is a chance for refactoring here. Since we're deserializing a bit prematurely here, we can just
    //       move the logic into the FluxProducer class since we have access to the pre-serialized record there
    //       and basically "inline" the buffering. I.e., all the buffering mechanisms + partition routing can be
    //       moved to FluxProducer. Come back to this in a later ticket/PR.
    /**
     * Extract partition information from a serialized ProducerRecord
     */
    private int extractPartitionFromRecord(byte[] serializedRecord) {
        // Deserialize to get the ProducerRecord and extract partition info
        ProducerRecord<String, String> record = ProducerRecordCodec.deserialize(
                serializedRecord, String.class, String.class);

        return PartitionSelector.getPartitionNumberForRecord(
                InMemoryTopicMetadataRepository.getInstance(),
                record.getPartitionNumber(),
                record.getKey(),
                record.getTopic(),
                numPartitions
        );
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
        // Check memory limits before proceeding
        if (totalBytesUsed + serializedRecord.length > maxBufferSize) {
            throw new IllegalStateException(
                "Cannot append record: would exceed maximum buffer size of " + maxBufferSize + " bytes. " +
                "Current usage: " + totalBytesUsed + " bytes, Record size: " + serializedRecord.length + " bytes."
            );
        }

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
            
            // Update total bytes used
            totalBytesUsed += serializedRecord.length;
            Logger.info("Record appended successfully to partition " + partition + ". Total bytes used: " + totalBytesUsed);
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
        return new ConcurrentHashMap<>(partitionBatches); // Return copy to prevent external modification
    }

    public int getBatchSize() {
        return batchSize;
    }

    public long getTotalBytesUsed() {
        return totalBytesUsed;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    /**
     * Helper method to decrease memory tracking when a batch is removed
     */
    private void decreaseMemoryUsage(RecordBatch batch) {
        totalBytesUsed -= batch.getCurrBatchSizeInBytes();
    }

    public void printRecord() {
        Logger.info("Batch Size: " + getBatchSize());
        Logger.info("Total Memory Used: " + totalBytesUsed + " / " + maxBufferSize + " bytes");
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
