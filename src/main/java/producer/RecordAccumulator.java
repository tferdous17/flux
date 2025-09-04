package producer;

import commons.utils.PartitionSelector;
import metadata.InMemoryTopicMetadataRepository;
import org.tinylog.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

public class RecordAccumulator {
    static private final int DEFAULT_BATCH_SIZE = 10_240; // 10 KB
    static private final long DEFAULT_LINGER_MS = 100; // 100ms default linger time
    static private final long DEFAULT_BUFFER_MEMORY = 32 * 1024 * 1024L; // 32MB default

    private final int batchSize;
    private final long lingerMs;
    private Map<Integer, RecordBatch> partitionBatches; // Per-partition batches
    private final int numPartitions;
    private final Object batchLock = new Object(); // For thread safety
    private final BufferPool bufferPool; // Memory management pool

    public RecordAccumulator(int numPartitions) {
        this(DEFAULT_BATCH_SIZE, numPartitions, DEFAULT_LINGER_MS);
    }

    public RecordAccumulator(int batchSize, int numPartitions) {
        this(batchSize, numPartitions, DEFAULT_LINGER_MS);
    }

    public RecordAccumulator(int batchSize, int numPartitions, long lingerMs) {
        this(batchSize, numPartitions, lingerMs, DEFAULT_BUFFER_MEMORY);
    }
    
    public RecordAccumulator(int batchSize, int numPartitions, long lingerMs, long bufferMemory) {
        this.batchSize = validateBatchSize(batchSize);
        this.lingerMs = lingerMs;
        this.partitionBatches = new HashMap<>();
        this.numPartitions = numPartitions;
        
        // Initialize the buffer pool with configured memory
        long maxBlockTimeMs = 1000; // 1 second max wait for memory
        this.bufferPool = new BufferPool(bufferMemory, batchSize, maxBlockTimeMs);
        Logger.info("RecordAccumulator initialized with BufferPool - Memory: {}MB, BatchSize: {}KB", 
                   bufferMemory / (1024 * 1024), batchSize / 1024);
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        // Create batch with buffer pool support
        return new RecordBatch(batchSize, bufferPool);
    }

    /**
     * Get batches that are ready for sending based on size or time
     * @return Map of partition to RecordBatch for ready batches
     */
    public Map<Integer, RecordBatch> getReadyBatches() {
        synchronized (batchLock) {
            Map<Integer, RecordBatch> readyBatches = new HashMap<>();
            
            for (Map.Entry<Integer, RecordBatch> entry : partitionBatches.entrySet()) {
                int partition = entry.getKey();
                RecordBatch batch = entry.getValue();
                
                if (batch != null && isBatchReady(batch)) {
                    readyBatches.put(partition, batch);
                }
            }
            
            // Remove ready batches from accumulator and release their buffers
            for (Map.Entry<Integer, RecordBatch> entry : readyBatches.entrySet()) {
                partitionBatches.remove(entry.getKey());
                // Note: Buffer release will be handled after the batch is sent
            }
            
            if (!readyBatches.isEmpty()) {
                Logger.info("Found " + readyBatches.size() + " ready batches");
            }
            
            return readyBatches;
        }
    }

    /**
     * Flush all current batches and return them for sending
     * @return Map of partition to RecordBatch for all non-empty batches
     */
    public Map<Integer, RecordBatch> flush() {
        synchronized (batchLock) {
            if (partitionBatches.isEmpty()) {
                Logger.info("No batches to flush");
                return new HashMap<>();
            }
            
            Logger.info("Flushing " + partitionBatches.size() + " partition batches");
            
            // Create a copy of current batches to return
            Map<Integer, RecordBatch> batchesToFlush = new HashMap<>(partitionBatches);
            
            // Clear the current batches since they're being flushed
            // Note: Buffer release will be handled after the batches are sent
            partitionBatches.clear();
            
            return batchesToFlush;
        }
    }

    /**
     * Check if a batch is ready for sending
     */
    private boolean isBatchReady(RecordBatch batch) {
        // Batch is ready if:
        // 1. It has records AND (batch is 90% full OR has expired)
        return batch.getRecordCount() > 0 && 
               (batch.getCurrBatchSizeInBytes() >= (batchSize * 0.9) || 
                batch.isExpired(lingerMs));
    }

    /**
     * Convert RecordBatch map to list of IntermediaryRecords for gRPC sending
     * @param batchMap Map of partition to RecordBatch
     * @return List of IntermediaryRecord ready for gRPC
     */
    public List<IntermediaryRecord> convertBatchesToIntermediaryRecords(Map<Integer, RecordBatch> batchMap) {
        List<IntermediaryRecord> records = new ArrayList<>();
        
        for (Map.Entry<Integer, RecordBatch> entry : batchMap.entrySet()) {
            RecordBatch batch = entry.getValue();
            
            // Use the new getIntermediaryRecords method from RecordBatch
            records.addAll(batch.getIntermediaryRecords());
        }
        
        return records;
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

    /**
     * Extract topic information from a serialized ProducerRecord
     */
    private String extractTopicFromRecord(byte[] serializedRecord) {
        // Deserialize to get the ProducerRecord and extract topic info
        ProducerRecord<String, String> record = ProducerRecordCodec.deserialize(
                serializedRecord, String.class, String.class);
        return record.getTopic();
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
        // Extract partition and topic from the serialized record
        int partition = extractPartitionFromRecord(serializedRecord);
        String topicName = extractTopicFromRecord(serializedRecord);
        
        synchronized (batchLock) {
            RecordBatch currentBatch = partitionBatches.get(partition);
            int baseOffset = 0; // TODO: Should be determined by broker/partition
            
            try {
                if (currentBatch == null || !currentBatch.append(serializedRecord, topicName, partition)) {
                    if (currentBatch != null) { // Case 1B - batch is full
                        Logger.info("Batch for partition " + partition + " is full. Creating new batch.");
                        // Note: We don't auto-flush here, let the caller decide when to flush
                    }
                    Logger.info("Creating a new batch for partition " + partition + ".");
                    currentBatch = createBatch(partition, baseOffset);
                    partitionBatches.put(partition, currentBatch);
                    
                    if (!currentBatch.append(serializedRecord, topicName, partition)) {
                        throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
                    }
                }
                Logger.info("Record appended successfully to partition " + partition + ".");
            } catch (Exception e) {
                Logger.error("Failed to append record: " + e.getMessage(), e);
                throw e;
            }
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
    
    /**
     * Get the buffer pool for monitoring or management
     */
    public BufferPool getBufferPool() {
        return bufferPool;
    }
    
    /**
     * Check if memory pressure exists
     */
    public boolean hasMemoryPressure() {
        return bufferPool != null && bufferPool.isUnderMemoryPressure();
    }
    
    /**
     * Release buffers for completed batches
     * Should be called after batches are successfully sent
     */
    public void releaseBatchBuffers(Map<Integer, RecordBatch> sentBatches) {
        if (sentBatches == null) {
            return;
        }
        
        for (RecordBatch batch : sentBatches.values()) {
            if (batch != null) {
                batch.releaseBuffer();
            }
        }
        Logger.debug("Released buffers for {} sent batches", sentBatches.size());
    }
    
    /**
     * Close the accumulator and release all resources
     */
    public void close() {
        synchronized (batchLock) {
            // Release all remaining batch buffers
            for (RecordBatch batch : partitionBatches.values()) {
                if (batch != null) {
                    batch.releaseBuffer();
                }
            }
            partitionBatches.clear();
            
            // Close the buffer pool
            if (bufferPool != null) {
                bufferPool.close();
            }
            
            Logger.info("RecordAccumulator closed and resources released");
        }
    }
}
