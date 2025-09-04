package producer;

import commons.utils.PartitionSelector;
import metadata.InMemoryTopicMetadataRepository;
import org.tinylog.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * RecordAccumulator manages the batching of producer records for efficient transmission.
 * 
 * <h3>Configuration Parameters:</h3>
 * <ul>
 *   <li><b>batch.size</b> - Maximum size in bytes for each batch (1 byte - 1MB, default: 10KB)</li>
 *   <li><b>linger.ms</b> - Maximum time to wait for additional records before sending (0-60000ms, default: 100ms)</li>
 *   <li><b>batch.timeout.ms</b> - Maximum time a batch can exist before forced sending (>= linger.ms, max: 300000ms, default: 30000ms)</li>
 *   <li><b>batch.size.threshold</b> - Percentage of batch size that triggers sending (0.0-1.0, default: 0.9)</li>
 *   <li><b>buffer.memory</b> - Total memory available for buffering (>= batch.size, max: 1GB, default: 32MB)</li>
 * </ul>
 * 
 * <h3>Batch Completion Triggers:</h3>
 * A batch is sent when any of these conditions are met:
 * <ol>
 *   <li>Batch size exceeds the configured threshold (e.g., 90% of max size)</li>
 *   <li>Batch has existed longer than the linger time</li>
 *   <li>Batch has existed longer than the maximum timeout (forced completion)</li>
 * </ol>
 * 
 * <h3>Thread Safety:</h3>
 * This class is thread-safe and can be used concurrently from multiple threads.
 * 
 * @see FluxProducer
 * @see RecordBatch
 * @see BufferPool
 */
public class RecordAccumulator {
    static private final int DEFAULT_BATCH_SIZE = 10_240; // 10 KB
    static private final long DEFAULT_LINGER_MS = 100; // 100ms default linger time
    static private final long DEFAULT_BATCH_TIMEOUT_MS = 30_000; // 30s default batch timeout
    static private final double DEFAULT_BATCH_SIZE_THRESHOLD = 0.9; // 90% default threshold
    static private final long DEFAULT_BUFFER_MEMORY = 32 * 1024 * 1024L; // 32MB default

    private final int batchSize;
    private final long lingerMs;
    private final long batchTimeoutMs;
    private final double batchSizeThreshold;
    private Map<Integer, RecordBatch> partitionBatches; // Per-partition batches
    private final int numPartitions;
    private final Object batchLock = new Object(); // For thread safety
    private final BufferPool bufferPool; // Memory management pool
    private final BatchCallbackRegistry callbackRegistry; // Callback management

    /**
     * Create a RecordAccumulator with default configuration.
     * Uses: 10KB batches, 100ms linger, 30s timeout, 90% threshold, 32MB memory
     * 
     * @param numPartitions Number of partitions to manage batches for
     */
    public RecordAccumulator(int numPartitions) {
        this(DEFAULT_BATCH_SIZE, numPartitions, DEFAULT_LINGER_MS, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, DEFAULT_BUFFER_MEMORY);
    }

    /**
     * Create a RecordAccumulator with custom batch size, other defaults.
     * Uses: custom batch size, 100ms linger, 30s timeout, 90% threshold, 32MB memory
     * 
     * @param batchSize Maximum size in bytes for each batch (1 byte - 1MB)
     * @param numPartitions Number of partitions to manage batches for
     */
    public RecordAccumulator(int batchSize, int numPartitions) {
        this(batchSize, numPartitions, DEFAULT_LINGER_MS, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, DEFAULT_BUFFER_MEMORY);
    }

    /**
     * Create a RecordAccumulator with custom batch size and linger time.
     * Uses: custom batch size/linger, 30s timeout, 90% threshold, 32MB memory
     * 
     * @param batchSize Maximum size in bytes for each batch (1 byte - 1MB)
     * @param numPartitions Number of partitions to manage batches for
     * @param lingerMs Maximum time in milliseconds to wait before sending (0-60000ms)
     */
    public RecordAccumulator(int batchSize, int numPartitions, long lingerMs) {
        this(batchSize, numPartitions, lingerMs, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, DEFAULT_BUFFER_MEMORY);
    }
    
    /**
     * Create a RecordAccumulator with custom batch size, linger time, and buffer memory.
     * Uses: custom batch size/linger/memory, 30s timeout, 90% threshold
     * 
     * @param batchSize Maximum size in bytes for each batch (1 byte - 1MB)
     * @param numPartitions Number of partitions to manage batches for
     * @param lingerMs Maximum time in milliseconds to wait before sending (0-60000ms)
     * @param bufferMemory Total memory in bytes available for buffering (>= batchSize, max 1GB)
     */
    public RecordAccumulator(int batchSize, int numPartitions, long lingerMs, long bufferMemory) {
        this(batchSize, numPartitions, lingerMs, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, bufferMemory);
    }
    
    /**
     * Create a RecordAccumulator with full configuration control.
     * 
     * @param batchSize Maximum size in bytes for each batch (1 byte - 1MB)
     * @param numPartitions Number of partitions to manage batches for
     * @param lingerMs Maximum time in milliseconds to wait for additional records 
     *                 before sending a batch (0-60000ms)
     * @param batchTimeoutMs Maximum time in milliseconds a batch can exist before
     *                       being forcibly sent, regardless of size (must be >= lingerMs, max 300000ms)
     * @param batchSizeThreshold Percentage (0.0-1.0) of batch size that triggers sending
     *                          (e.g., 0.9 = send when batch is 90% full)
     * @param bufferMemory Total memory in bytes available for buffering
     *                     (must be >= batchSize, max 1GB)
     * @throws IllegalArgumentException if any parameter is invalid
     */
    public RecordAccumulator(int batchSize, int numPartitions, long lingerMs, long batchTimeoutMs, double batchSizeThreshold, long bufferMemory) {
        // Validate all configuration parameters
        validateConfiguration(batchSize, lingerMs, batchTimeoutMs, batchSizeThreshold, bufferMemory);
        
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.batchTimeoutMs = batchTimeoutMs;
        this.batchSizeThreshold = batchSizeThreshold;
        this.partitionBatches = new HashMap<>();
        this.numPartitions = numPartitions;
        
        // Initialize the buffer pool with configured memory
        long maxBlockTimeMs = 1000; // 1 second max wait for memory
        this.bufferPool = new BufferPool(bufferMemory, batchSize, maxBlockTimeMs);
        
        // Initialize the callback registry
        this.callbackRegistry = new BatchCallbackRegistry();
        
        Logger.info("RecordAccumulator initialized - BatchSize: {}KB, LingerMs: {}ms, BatchTimeout: {}ms, SizeThreshold: {}, Memory: {}MB", 
                   batchSize / 1024, lingerMs, batchTimeoutMs, batchSizeThreshold, bufferMemory / (1024 * 1024));
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        
        // Create batch with buffer pool support
        RecordBatch batch = new RecordBatch(batchSize, bufferPool);
        
        // Add default callbacks for resource management and logging
        batch.addCallback(new BufferReleaseCallback());
        batch.addCallback(new LoggingCallback());
        
        // Notify that batch was created
        batch.notifyBatchCreated();
        callbackRegistry.executeBatchCreatedCallbacks(batch.getBatchInfo());
        
        return batch;
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
            
            // Remove ready batches from accumulator and notify callbacks
            for (Map.Entry<Integer, RecordBatch> entry : readyBatches.entrySet()) {
                RecordBatch batch = entry.getValue();
                partitionBatches.remove(entry.getKey());
                
                // Update batch state and notify callbacks
                batch.setState(BatchState.READY);
                batch.notifyBatchReady();
                callbackRegistry.executeBatchReadyCallbacks(batch.getBatchInfo());
                
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
            
            // Update batch states and notify callbacks before clearing
            for (RecordBatch batch : batchesToFlush.values()) {
                if (batch != null && batch.getRecordCount() > 0) {
                    batch.setState(BatchState.READY);
                    batch.notifyBatchReady();
                    callbackRegistry.executeBatchReadyCallbacks(batch.getBatchInfo());
                }
            }
            
            // Clear the current batches since they're being flushed
            // Note: Buffer release will be handled after the batches are sent
            partitionBatches.clear();
            
            return batchesToFlush;
        }
    }

    /**
     * Check if a batch is ready for sending
     * A batch is ready if it has records AND meets any of these conditions:
     * 1. Batch size exceeds the configured threshold (default 90%)
     * 2. Batch has exceeded the linger time (immediate readiness after wait)
     * 3. Batch has exceeded the maximum timeout (forced completion)
     */
    private boolean isBatchReady(RecordBatch batch) {
        if (batch.getRecordCount() == 0) {
            return false; // Never send empty batches
        }
        
        // Check size threshold
        if (batch.getCurrBatchSizeInBytes() >= (batchSize * batchSizeThreshold)) {
            return true;
        }
        
        // Check linger time expiration
        if (batch.isExpired(lingerMs)) {
            return true;
        }
        
        // Check maximum batch timeout (force completion)
        if (batch.isExpired(batchTimeoutMs)) {
            Logger.warn("Batch {} exceeded maximum timeout ({}ms), forcing completion", 
                       batch.getBatchId(), batchTimeoutMs);
            return true;
        }
        
        return false;
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

    /**
     * Get the configured batch size in bytes.
     * 
     * @return Maximum size in bytes for each batch
     */
    public int getBatchSize() {
        return batchSize;
    }
    
    /**
     * Get the configured linger time in milliseconds.
     * This is the maximum time to wait for additional records before sending a batch.
     * 
     * @return Linger time in milliseconds
     */
    public long getLingerMs() {
        return lingerMs;
    }
    
    /**
     * Get the configured batch timeout in milliseconds.
     * This is the maximum time a batch can exist before being forcibly sent.
     * 
     * @return Batch timeout in milliseconds
     */
    public long getBatchTimeoutMs() {
        return batchTimeoutMs;
    }
    
    /**
     * Get the configured batch size threshold.
     * This is the percentage of batch size that triggers sending (e.g., 0.9 = 90%).
     * 
     * @return Batch size threshold as a decimal between 0.0 and 1.0
     */
    public double getBatchSizeThreshold() {
        return batchSizeThreshold;
    }

    public void printRecord() {
        Logger.info("Batch Size: " + getBatchSize());
        Logger.info("Partition Batches:");
        
        partitionBatches.forEach((partition, batch) -> {
            Logger.info("Partition " + partition + ":");
            batch.printBatchDetails();
        });
    }

    /**
     * Validate all configuration parameters
     */
    private void validateConfiguration(int batchSize, long lingerMs, long batchTimeoutMs, double batchSizeThreshold, long bufferMemory) {
        validateBatchSize(batchSize);
        validateLingerMs(lingerMs);
        validateBatchTimeoutMs(batchTimeoutMs, lingerMs);
        validateBatchSizeThreshold(batchSizeThreshold);
        validateBufferMemory(bufferMemory, batchSize);
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
    
    private void validateLingerMs(long lingerMs) {
        final long MIN_LINGER_MS = 0; // No waiting
        final long MAX_LINGER_MS = 60_000; // 1 minute max
        
        if (lingerMs < MIN_LINGER_MS || lingerMs > MAX_LINGER_MS) {
            throw new IllegalArgumentException(
                    "Linger time must be between " + MIN_LINGER_MS + "-" + MAX_LINGER_MS + " milliseconds."
            );
        }
    }
    
    private void validateBatchTimeoutMs(long batchTimeoutMs, long lingerMs) {
        final long MAX_BATCH_TIMEOUT_MS = 300_000; // 5 minutes max
        
        if (batchTimeoutMs < lingerMs) {
            throw new IllegalArgumentException(
                    "Batch timeout (" + batchTimeoutMs + "ms) must be greater than or equal to linger time (" + lingerMs + "ms)."
            );
        }
        
        if (batchTimeoutMs > MAX_BATCH_TIMEOUT_MS) {
            throw new IllegalArgumentException(
                    "Batch timeout must not exceed " + MAX_BATCH_TIMEOUT_MS + " milliseconds."
            );
        }
    }
    
    private void validateBatchSizeThreshold(double batchSizeThreshold) {
        if (batchSizeThreshold <= 0.0 || batchSizeThreshold > 1.0) {
            throw new IllegalArgumentException(
                    "Batch size threshold must be between 0.0 and 1.0 (exclusive of 0.0, inclusive of 1.0)."
            );
        }
    }
    
    private void validateBufferMemory(long bufferMemory, int batchSize) {
        final long MAX_BUFFER_MEMORY = 1_073_741_824L; // 1GB
        
        if (bufferMemory < batchSize) {
            throw new IllegalArgumentException(
                    "Buffer memory (" + bufferMemory + " bytes) must be at least as large as batch size (" + batchSize + " bytes)."
            );
        }
        
        if (bufferMemory > MAX_BUFFER_MEMORY) {
            throw new IllegalArgumentException(
                    "Buffer memory must not exceed " + MAX_BUFFER_MEMORY + " bytes (1GB)."
            );
        }
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
     * Add a callback to be notified about batch lifecycle events.
     *
     * @param batchId The batch ID to register the callback for
     * @param callback The callback to register
     */
    public void addBatchCallback(String batchId, BatchCallback callback) {
        callbackRegistry.registerCallback(batchId, callback);
    }
    
    /**
     * Remove a callback for a specific batch.
     *
     * @param batchId The batch ID
     * @param callback The callback to remove
     * @return true if the callback was removed
     */
    public boolean removeBatchCallback(String batchId, BatchCallback callback) {
        return callbackRegistry.unregisterCallback(batchId, callback);
    }
    
    /**
     * Notify callbacks that a batch is being sent.
     *
     * @param batchId The batch being sent
     */
    public void onBatchSending(String batchId) {
        // Find the batch and update its state
        synchronized (batchLock) {
            for (RecordBatch batch : partitionBatches.values()) {
                if (batch != null && batch.getBatchId().equals(batchId)) {
                    batch.setState(BatchState.SENDING);
                    batch.setSentTime(System.currentTimeMillis());
                    batch.notifyBatchSending();
                    callbackRegistry.executeBatchSendingCallbacks(batch.getBatchInfo());
                    break;
                }
            }
        }
    }
    
    /**
     * Notify callbacks that a batch has been successfully sent.
     *
     * @param batchId The batch that was sent successfully
     * @param result The broker acknowledgment/result
     */
    public void onBatchSendSuccess(String batchId, Object result) {
        // Find the batch and notify success
        RecordBatch batch = findBatchById(batchId);
        if (batch != null) {
            batch.setState(BatchState.COMPLETED);
            batch.notifyBatchSuccess(result);
            callbackRegistry.executeBatchSuccessCallbacks(batch.getBatchInfo(), result);
        } else {
            Logger.warn("Could not find batch {} for success callback", batchId);
        }
    }
    
    /**
     * Notify callbacks that a batch has failed to send.
     *
     * @param batchId The batch that failed to send
     * @param exception The error that caused the failure
     */
    public void onBatchSendFailure(String batchId, Throwable exception) {
        // Find the batch and notify failure
        RecordBatch batch = findBatchById(batchId);
        if (batch != null) {
            batch.setState(BatchState.FAILED);
            batch.notifyBatchFailure(exception);
            callbackRegistry.executeBatchFailureCallbacks(batch.getBatchInfo(), exception);
        } else {
            Logger.warn("Could not find batch {} for failure callback", batchId);
        }
    }
    
    /**
     * Find a batch by its ID across all partitions.
     * This is used for callback notifications when we only have the batch ID.
     */
    private RecordBatch findBatchById(String batchId) {
        synchronized (batchLock) {
            for (RecordBatch batch : partitionBatches.values()) {
                if (batch != null && batch.getBatchId().equals(batchId)) {
                    return batch;
                }
            }
        }
        return null;
    }
    
    /**
     * Get callback registry statistics.
     *
     * @return String containing callback registry stats
     */
    public String getCallbackStats() {
        return String.format("Callback Registry Stats - Batches: %d, Total Callbacks: %d, Active: %d",
                           callbackRegistry.getBatchCount(),
                           callbackRegistry.getTotalCallbackCount(),
                           callbackRegistry.getActiveCallbackCount());
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
            
            // Close the callback registry
            if (callbackRegistry != null) {
                callbackRegistry.shutdown();
            }
            
            // Close the buffer pool
            if (bufferPool != null) {
                bufferPool.close();
            }
            
            Logger.info("RecordAccumulator closed and resources released");
        }
    }
}
