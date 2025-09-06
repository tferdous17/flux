package producer;

import commons.compression.CompressionType;
import commons.utils.PartitionSelector;
import metadata.InMemoryTopicMetadataRepository;
import org.tinylog.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batches producer records for efficient transmission.
 * Thread-safe for concurrent use.
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
    private final ConcurrentHashMap<Integer, RecordBatch> partitionBatches; // Thread-safe per-partition batches
    private final ConcurrentHashMap<String, RecordBatch> inflightBatches; // Track batches that have been flushed but not completed
    private final int numPartitions;
    private final Object batchLock = new Object(); // For thread safety
    private final BufferPool bufferPool; // Memory management pool
    private final CompressionType compressionType; // Compression type for all batches
    
    // Simple metrics
    private final AtomicLong batchesCreated = new AtomicLong(0);
    private final AtomicLong batchesSent = new AtomicLong(0);
    private final AtomicLong batchesFailed = new AtomicLong(0);
    private final AtomicLong recordsAppended = new AtomicLong(0);

    /**
     * Create a RecordAccumulator with default configuration.
     * Uses: 10KB batches, 100ms linger, 30s timeout, 90% threshold, 32MB memory
     * 
     * @param numPartitions Number of partitions to manage batches for
     */
    public RecordAccumulator(int numPartitions) {
        this(DEFAULT_BATCH_SIZE, numPartitions, DEFAULT_LINGER_MS, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, DEFAULT_BUFFER_MEMORY, CompressionType.NONE);
    }

    /**
     * Create a RecordAccumulator with custom batch size, other defaults.
     * Uses: custom batch size, 100ms linger, 30s timeout, 90% threshold, 32MB memory
     * 
     * @param batchSize Maximum size in bytes for each batch (1 byte - 1MB)
     * @param numPartitions Number of partitions to manage batches for
     */
    public RecordAccumulator(int batchSize, int numPartitions) {
        this(batchSize, numPartitions, DEFAULT_LINGER_MS, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, DEFAULT_BUFFER_MEMORY, CompressionType.NONE);
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
        this(batchSize, numPartitions, lingerMs, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, DEFAULT_BUFFER_MEMORY, CompressionType.NONE);
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
        this(batchSize, numPartitions, lingerMs, DEFAULT_BATCH_TIMEOUT_MS, DEFAULT_BATCH_SIZE_THRESHOLD, bufferMemory, CompressionType.NONE);
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
    public RecordAccumulator(int batchSize, int numPartitions, long lingerMs, long batchTimeoutMs, double batchSizeThreshold, long bufferMemory, CompressionType compressionType) {
        // Validate all configuration parameters
        validateConfiguration(batchSize, lingerMs, batchTimeoutMs, batchSizeThreshold, bufferMemory);
        
        this.batchSize = batchSize;
        this.lingerMs = lingerMs;
        this.batchTimeoutMs = batchTimeoutMs;
        this.batchSizeThreshold = batchSizeThreshold;
        this.partitionBatches = new ConcurrentHashMap<>();
        this.inflightBatches = new ConcurrentHashMap<>();
        this.numPartitions = numPartitions;
        this.compressionType = compressionType != null ? compressionType : CompressionType.NONE;
        
        // Initialize the buffer pool with configured memory
        long maxBlockTimeMs = 1000; // 1 second max wait for memory
        this.bufferPool = new BufferPool(bufferMemory, batchSize, maxBlockTimeMs);
        
        
        Logger.info("RecordAccumulator initialized - BatchSize: {}KB, LingerMs: {}ms, BatchTimeout: {}ms, SizeThreshold: {}, Memory: {}MB, Compression: {}", 
                   batchSize / 1024, lingerMs, batchTimeoutMs, batchSizeThreshold, bufferMemory / (1024 * 1024), compressionType);
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        
        // Create batch with buffer pool support and compression
        RecordBatch batch = new RecordBatch(batchSize, bufferPool, compressionType);
        batchesCreated.incrementAndGet();
        return batch;
    }

    /**
     * Drain ready batches from the accumulator for sending.
     * This method identifies batches that are ready for transmission, removes them from
     * the accumulator, and tracks them as in-flight. This follows Kafka's drain pattern.
     * 
     * A batch is considered ready if:
     * - Its size exceeds the configured threshold, OR
     * - It has exceeded the linger time, OR  
     * - It has exceeded the maximum batch timeout
     * 
     * @return Map of partition to RecordBatch for batches ready to send
     */
    public Map<Integer, RecordBatch> drain() {
        Map<Integer, RecordBatch> drainedBatches = new ConcurrentHashMap<>();
        
        // First pass: identify and atomically remove ready batches
        for (Map.Entry<Integer, RecordBatch> entry : partitionBatches.entrySet()) {
            int partition = entry.getKey();
            RecordBatch batch = entry.getValue();
            
            if (batch != null && isBatchReady(batch)) {
                // Atomically remove from partitionBatches if still present
                if (partitionBatches.remove(partition, batch)) {
                    drainedBatches.put(partition, batch);
                }
            }
        }
        
        // Second pass: track drained batches as in-flight
        for (Map.Entry<Integer, RecordBatch> entry : drainedBatches.entrySet()) {
            RecordBatch batch = entry.getValue();
            inflightBatches.put(batch.getBatchId(), batch);
        }
        
        if (!drainedBatches.isEmpty()) {
            Logger.info("Drained {} ready batches for sending", drainedBatches.size());
        }
        
        return drainedBatches;
    }
    
    /**
     * Get batches that are ready for sending based on size or time.
     * This method is kept for backward compatibility but delegates to drain().
     * 
     * @deprecated Use drain() instead for better Kafka compatibility
     * @return Map of partition to RecordBatch for ready batches
     */
    @Deprecated
    public Map<Integer, RecordBatch> getReadyBatches() {
        return drain();
    }

    /**
     * Flush all current batches and return them for sending.
     * Unlike drain(), this method sends ALL batches regardless of readiness.
     * This is typically used for forced flushes (e.g., shutdown, partition changes).
     * 
     * Key differences from drain():
     * - drain(): Only sends ready batches (size/time triggered)
     * - flush(): Sends ALL non-empty batches immediately
     * 
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
            
            // Update batch states before clearing
            for (RecordBatch batch : batchesToFlush.values()) {
                if (batch != null && batch.getRecordCount() > 0) {
                    // Track this batch as in-flight
                    inflightBatches.put(batch.getBatchId(), batch);
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
                boolean appended = currentBatch != null && currentBatch.append(serializedRecord, topicName, partition);
                if (!appended) {
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
                    recordsAppended.incrementAndGet();
                } else {
                    recordsAppended.incrementAndGet();
                }
                Logger.info("Record appended successfully to partition " + partition + ".");
            } catch (Exception e) {
                Logger.error("Failed to append record: " + e.getMessage(), e);
                throw e;
            }
        }
    }
    
    /**
     * Append a record with callback support and return future
     * @param serializedRecord the serialized record data
     * @param callback optional callback to invoke when record is sent
     * @return RecordAppendResult containing future and batch information
     */
    public RecordAppendResult append(byte[] serializedRecord, Callback callback) throws IOException {
        // Extract partition and topic from the serialized record
        int partition = extractPartitionFromRecord(serializedRecord);
        String topicName = extractTopicFromRecord(serializedRecord);
        
        RecordFuture future = new RecordFuture(callback);
        boolean newBatchCreated = false;
        boolean batchIsFull = false;
        
        synchronized (batchLock) {
            RecordBatch currentBatch = partitionBatches.get(partition);
            int baseOffset = 0; // TODO: Should be determined by broker/partition
            
            try {
                boolean appended = currentBatch != null && currentBatch.append(serializedRecord, topicName, partition, future);
                if (!appended) {
                    if (currentBatch != null) { // Case 1B - batch is full
                        Logger.info("Batch for partition " + partition + " is full. Creating new batch.");
                        batchIsFull = true;
                    }
                    Logger.info("Creating a new batch for partition " + partition + ".");
                    currentBatch = createBatch(partition, baseOffset);
                    partitionBatches.put(partition, currentBatch);
                    newBatchCreated = true;
                    
                    if (!currentBatch.append(serializedRecord, topicName, partition, future)) {
                        future.completeExceptionally(new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration."));
                        throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
                    }
                    recordsAppended.incrementAndGet();
                } else {
                    recordsAppended.incrementAndGet();
                }
                Logger.info("Record appended successfully to partition " + partition + ".");
                
                return new RecordAppendResult(future, batchIsFull, newBatchCreated);
            } catch (Exception e) {
                future.completeExceptionally(e);
                Logger.error("Failed to append record: " + e.getMessage(), e);
                throw e;
            }
        }
    }

    /**
     * Get the current batch for a specific partition
     * Thread-safe operation using ConcurrentHashMap
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
     * @return A copy of the current partition batches to prevent external modification
     */
    public Map<Integer, RecordBatch> getPartitionBatches() {
        return new ConcurrentHashMap<>(partitionBatches); // Return thread-safe copy
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
     * Mark a batch as being sent.
     *
     * @param batchId The batch being sent
     */
    public void markBatchSending(String batchId) {
        RecordBatch batch = findBatchById(batchId);
        if (batch != null) {
            batch.setSentTime(System.currentTimeMillis());
            Logger.debug("Batch {} marked as SENDING", batchId);
        } else {
            Logger.warn("Could not find batch {} to mark as sending", batchId);
        }
    }
    
    /**
     * Mark a batch as successfully sent and remove from inflight tracking.
     *
     * @param batchId The batch that was sent successfully
     */
    public void markBatchSuccess(String batchId) {
        markBatchSuccess(batchId, 0L); // Default offset
    }
    
    /**
     * Mark a batch as successfully sent with specific offset
     * 
     * @param batchId The batch that was sent successfully
     * @param baseOffset The base offset assigned by the broker
     */
    public void markBatchSuccess(String batchId, long baseOffset) {
        RecordBatch batch = findBatchById(batchId);
        if (batch != null) {
            // Complete all futures in the batch
            batch.complete(baseOffset);
            
            batch.releaseBuffer(); // Release buffer back to pool
            inflightBatches.remove(batchId);
            batchesSent.incrementAndGet();
            Logger.info("Batch {} completed successfully with base offset {}", batchId, baseOffset);
        } else {
            Logger.warn("Could not find batch {} to mark as successful", batchId);
        }
    }
    
    /**
     * Mark a batch as failed and remove from inflight tracking.
     *
     * @param batchId The batch that failed to send
     * @param exception The error that caused the failure
     */
    public void markBatchFailure(String batchId, Throwable exception) {
        RecordBatch batch = findBatchById(batchId);
        if (batch != null) {
            // Complete all futures in the batch with exception
            batch.completeExceptionally(exception instanceof Exception ? 
                (Exception) exception : new Exception(exception));
            
            batch.releaseBuffer(); // Release buffer back to pool
            inflightBatches.remove(batchId);
            batchesFailed.incrementAndGet();
            Logger.error("Batch {} failed: {}", batchId, exception.getMessage());
        } else {
            Logger.warn("Could not find batch {} to mark as failed", batchId);
        }
    }
    
    /**
     * Find a batch by its ID across all partitions.
     * This is used for callback notifications when we only have the batch ID.
     */
    private RecordBatch findBatchById(String batchId) {
        // First check in-flight batches (batches that have been flushed)
        RecordBatch batch = inflightBatches.get(batchId);
        if (batch != null) {
            return batch;
        }
        
        // Then check current partition batches
        synchronized (batchLock) {
            for (RecordBatch b : partitionBatches.values()) {
                if (b != null && b.getBatchId().equals(batchId)) {
                    return b;
                }
            }
        }
        return null;
    }
    
    /**
     * Get simple metrics for monitoring.
     * 
     * @return Metrics object with current stats
     */
    public AccumulatorMetrics getMetrics() {
        return new AccumulatorMetrics(
            batchesCreated.get(),
            batchesSent.get(),
            batchesFailed.get(),
            recordsAppended.get(),
            inflightBatches.size(),
            bufferPool.availableMemory(),
            bufferPool.totalMemory()
        );
    }
    
    /**
     * Simple metrics class for monitoring.
     */
    public static class AccumulatorMetrics {
        public final long batchesCreated;
        public final long batchesSent;
        public final long batchesFailed;
        public final long recordsAppended;
        public final int inflightBatches;
        public final long availableMemory;
        public final long totalMemory;
        
        public AccumulatorMetrics(long batchesCreated, long batchesSent, long batchesFailed,
                                 long recordsAppended, int inflightBatches, 
                                 long availableMemory, long totalMemory) {
            this.batchesCreated = batchesCreated;
            this.batchesSent = batchesSent;
            this.batchesFailed = batchesFailed;
            this.recordsAppended = recordsAppended;
            this.inflightBatches = inflightBatches;
            this.availableMemory = availableMemory;
            this.totalMemory = totalMemory;
        }
        
        @Override
        public String toString() {
            return String.format(
                "Metrics[batches=(created=%d, sent=%d, failed=%d), records=%d, inflight=%d, memory=%d/%d]",
                batchesCreated, batchesSent, batchesFailed, recordsAppended, 
                inflightBatches, availableMemory, totalMemory
            );
        }
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
            
            // Release all in-flight batch buffers
            for (RecordBatch batch : inflightBatches.values()) {
                if (batch != null) {
                    batch.releaseBuffer();
                }
            }
            inflightBatches.clear();
            
            
            // Close the buffer pool
            if (bufferPool != null) {
                bufferPool.close();
            }
            
            Logger.info("RecordAccumulator closed and resources released");
        }
    }
}
