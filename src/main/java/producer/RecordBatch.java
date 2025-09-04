package producer;

import org.tinylog.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

public class RecordBatch {
    private final int maxBatchSizeInBytes;
    private int currBatchSizeInBytes;
    private int numRecords;
    private ByteBuffer batch;
    private final List<RecordMetadata> recordMetadataList; // Store metadata for each record
    private final long createdTimeMs; // For batch timeout tracking
    private final BufferPool bufferPool; // Optional buffer pool for memory management
    private final boolean pooledBuffer; // Track if this buffer came from a pool
    
    // Callback-related fields
    private final String batchId; // Unique identifier for this batch
    private final List<BatchCallback> callbacks; // Thread-safe callback list
    private volatile BatchState state; // Current state of the batch
    private volatile Long sentTimeMs; // Timestamp when batch was sent
    private volatile String topicName; // Primary topic for this batch

    private static final int DEFAULT_BATCH_SIZE = 10_240; // default batch size: 10 KB = 10,240 bytes

    /**
     * Metadata for individual records within the batch
     */
    public static class RecordMetadata {
        public final String topicName;
        public final int partition;
        public final int recordLength;
        public final int offsetInBatch;

        public RecordMetadata(String topicName, int partition, int recordLength, int offsetInBatch) {
            this.topicName = topicName;
            this.partition = partition;
            this.recordLength = recordLength;
            this.offsetInBatch = offsetInBatch;
        }
    }

    public RecordBatch() {
        this(DEFAULT_BATCH_SIZE);
    }

    public RecordBatch(int maxBatchSizeInBytes) {
        this(maxBatchSizeInBytes, null);
    }
    
    /**
     * Create a RecordBatch with optional BufferPool support
     * @param maxBatchSizeInBytes Maximum size of the batch
     * @param bufferPool Optional buffer pool for memory management
     */
    public RecordBatch(int maxBatchSizeInBytes, BufferPool bufferPool) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.currBatchSizeInBytes = 0;
        this.numRecords = 0;
        this.bufferPool = bufferPool;
        this.recordMetadataList = new ArrayList<>();
        this.createdTimeMs = System.currentTimeMillis();
        
        // Initialize callback-related fields
        this.batchId = UUID.randomUUID().toString();
        this.callbacks = new CopyOnWriteArrayList<>();
        this.state = BatchState.CREATED;
        this.sentTimeMs = null;
        this.topicName = null;
        
        // Try to get buffer from pool if available
        boolean isPooled = false;
        ByteBuffer batchBuffer = null;
        
        if (bufferPool != null) {
            try {
                batchBuffer = bufferPool.allocate(maxBatchSizeInBytes);
                isPooled = true;
                Logger.debug("Allocated batch buffer from pool (size: {} bytes)", maxBatchSizeInBytes);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Logger.warn("Interrupted while allocating buffer from pool, using direct allocation");
                batchBuffer = ByteBuffer.allocate(maxBatchSizeInBytes);
                isPooled = false;
            } catch (Exception e) {
                Logger.warn("Failed to allocate buffer from pool: {}, using direct allocation", e.getMessage());
                batchBuffer = ByteBuffer.allocate(maxBatchSizeInBytes);
                isPooled = false;
            }
        } else {
            batchBuffer = ByteBuffer.allocate(maxBatchSizeInBytes);
            isPooled = false;
        }
        
        this.batch = batchBuffer;
        this.pooledBuffer = isPooled;
    }

    // NOTE: This method may need refactoring once SerializedRecord (Producer) is implemented, but logic remains same
    public boolean append(byte[] serializedRecord) throws IOException {
        // Default method - extract topic/partition from record if possible
        return append(serializedRecord, null, -1);
    }

    /**
     * Append a record with metadata to the batch
     * @param serializedRecord the serialized record data
     * @param topicName topic name for the record
     * @param partition partition for the record
     * @return true if record fits, false if batch is full
     */
    public boolean append(byte[] serializedRecord, String topicName, int partition) throws IOException {
        // Check if record can fit in the current batch
        if (currBatchSizeInBytes + serializedRecord.length > maxBatchSizeInBytes) {
            Logger.warn("Record can not fit in current batch. New one may be necessary");
            return false;
        }
        
        // Set topic name if this is the first record
        if (this.topicName == null && topicName != null) {
            this.topicName = topicName;
        }
        
        // Store metadata for this record
        int offsetInBatch = currBatchSizeInBytes;
        RecordMetadata metadata = new RecordMetadata(topicName, partition, serializedRecord.length, offsetInBatch);
        recordMetadataList.add(metadata);
        
        // Add record to batch
        batch.put(serializedRecord);
        currBatchSizeInBytes += serializedRecord.length;
        numRecords++;

        return true;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public int getCurrBatchSizeInBytes() {
        return currBatchSizeInBytes;
    }

    public int getRecordCount() {
        return numRecords;
    }

    public ByteBuffer getBatchBuffer() {
        return batch;
    }

    public List<RecordMetadata> getRecordMetadata() {
        return new ArrayList<>(recordMetadataList); // Return copy to prevent external modification
    }

    public long getCreatedTimeMs() {
        return createdTimeMs;
    }

    public boolean isExpired(long timeoutMs) {
        return System.currentTimeMillis() - createdTimeMs > timeoutMs;
    }

    /**
     * Get individual IntermediaryRecord objects from this batch
     * @return List of IntermediaryRecord objects
     */
    public List<IntermediaryRecord> getIntermediaryRecords() {
        List<IntermediaryRecord> records = new ArrayList<>();
        ByteBuffer readBuffer = batch.duplicate(); // Create a read-only view
        readBuffer.rewind(); // Reset position to start
        
        for (RecordMetadata metadata : recordMetadataList) {
            byte[] recordData = new byte[metadata.recordLength];
            readBuffer.position(metadata.offsetInBatch);
            readBuffer.get(recordData);
            
            records.add(new IntermediaryRecord(metadata.topicName, metadata.partition, recordData));
        }
        
        return records;
    }

    public void printBatchDetails() {
        System.out.println("Batch ID: " + batchId);
        System.out.println("Number of Records: " + getRecordCount());
        System.out.println("Current Size: " + currBatchSizeInBytes + " bytes");
        System.out.println("Max Batch Size: " + maxBatchSizeInBytes + " bytes");
        System.out.println("Created: " + createdTimeMs + "ms ago");
        System.out.println("State: " + state);
        System.out.println("Topic: " + topicName);
        System.out.println("Callbacks: " + callbacks.size() + "\n");
    }
    
    // ======================== Callback-related methods ========================
    
    /**
     * Get the unique identifier for this batch.
     *
     * @return The batch ID
     */
    public String getBatchId() {
        return batchId;
    }
    
    /**
     * Get the current state of this batch.
     *
     * @return Current batch state
     */
    public BatchState getState() {
        return state;
    }
    
    /**
     * Set the state of this batch with validation.
     *
     * @param newState The new state to transition to
     * @throws IllegalStateException if the transition is invalid
     */
    public void setState(BatchState newState) {
        if (this.state != newState) {
            this.state.validateTransition(newState);
            BatchState oldState = this.state;
            this.state = newState;
            Logger.debug("Batch {} state changed from {} to {}", batchId, oldState, newState);
        }
    }
    
    /**
     * Get the topic name for this batch.
     *
     * @return Topic name, or null if not set
     */
    public String getTopicName() {
        return topicName;
    }
    
    /**
     * Set the sent timestamp for this batch.
     *
     * @param sentTimeMs Timestamp when the batch was sent
     */
    public void setSentTime(long sentTimeMs) {
        this.sentTimeMs = sentTimeMs;
    }
    
    /**
     * Get the sent timestamp for this batch.
     *
     * @return Sent timestamp, or null if not sent yet
     */
    public Long getSentTimeMs() {
        return sentTimeMs;
    }
    
    /**
     * Add a callback to this batch.
     *
     * @param callback The callback to add
     */
    public void addCallback(BatchCallback callback) {
        if (callback != null) {
            callbacks.add(callback);
            Logger.debug("Added callback to batch {}: {}", batchId, callback.getClass().getSimpleName());
        }
    }
    
    /**
     * Remove a callback from this batch.
     *
     * @param callback The callback to remove
     * @return true if the callback was removed
     */
    public boolean removeCallback(BatchCallback callback) {
        if (callback != null) {
            boolean removed = callbacks.remove(callback);
            if (removed) {
                Logger.debug("Removed callback from batch {}: {}", batchId, callback.getClass().getSimpleName());
            }
            return removed;
        }
        return false;
    }
    
    /**
     * Get a copy of the callback list.
     *
     * @return List of callbacks for this batch
     */
    public List<BatchCallback> getCallbacks() {
        return new ArrayList<>(callbacks);
    }
    
    /**
     * Create a BatchInfo snapshot for this batch.
     *
     * @return Immutable BatchInfo representing current state
     */
    public BatchInfo getBatchInfo() {
        return new BatchInfo(
            batchId,
            recordMetadataList.isEmpty() ? -1 : recordMetadataList.get(0).partition,
            numRecords,
            currBatchSizeInBytes,
            createdTimeMs,
            sentTimeMs,
            topicName,
            state
        );
    }
    
    /**
     * Notify all registered callbacks about a batch created event.
     */
    public void notifyBatchCreated() {
        if (!callbacks.isEmpty()) {
            BatchInfo info = getBatchInfo();
            Logger.debug("Notifying {} callbacks about batch created: {}", callbacks.size(), batchId);
            
            for (BatchCallback callback : callbacks) {
                try {
                    callback.onBatchCreated(info);
                } catch (Exception e) {
                    Logger.error("Callback failed for batch created {}: {}", batchId, e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * Notify all registered callbacks about a batch ready event.
     */
    public void notifyBatchReady() {
        if (!callbacks.isEmpty()) {
            BatchInfo info = getBatchInfo();
            Logger.debug("Notifying {} callbacks about batch ready: {}", callbacks.size(), batchId);
            
            for (BatchCallback callback : callbacks) {
                try {
                    callback.onBatchReady(info);
                } catch (Exception e) {
                    Logger.error("Callback failed for batch ready {}: {}", batchId, e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * Notify all registered callbacks about a batch sending event.
     */
    public void notifyBatchSending() {
        if (!callbacks.isEmpty()) {
            BatchInfo info = getBatchInfo();
            Logger.debug("Notifying {} callbacks about batch sending: {}", callbacks.size(), batchId);
            
            for (BatchCallback callback : callbacks) {
                try {
                    callback.onBatchSending(info);
                } catch (Exception e) {
                    Logger.error("Callback failed for batch sending {}: {}", batchId, e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * Notify all registered callbacks about a batch success event.
     *
     * @param result The broker acknowledgment/result
     */
    public void notifyBatchSuccess(Object result) {
        if (!callbacks.isEmpty()) {
            BatchInfo info = getBatchInfo();
            Logger.debug("Notifying {} callbacks about batch success: {}", callbacks.size(), batchId);
            
            for (BatchCallback callback : callbacks) {
                try {
                    callback.onBatchSuccess(info, result);
                } catch (Exception e) {
                    Logger.error("Callback failed for batch success {}: {}", batchId, e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * Notify all registered callbacks about a batch failure event.
     *
     * @param exception The error that caused the failure
     */
    public void notifyBatchFailure(Throwable exception) {
        if (!callbacks.isEmpty()) {
            BatchInfo info = getBatchInfo();
            Logger.debug("Notifying {} callbacks about batch failure: {}", callbacks.size(), batchId);
            
            for (BatchCallback callback : callbacks) {
                try {
                    callback.onBatchFailure(info, exception);
                } catch (Exception e) {
                    Logger.error("Callback failed for batch failure {}: {}", batchId, e.getMessage(), e);
                }
            }
        }
    }
    
    /**
     * Release the buffer back to the pool if it was allocated from one
     * This should be called when the batch is sent or discarded
     */
    public void releaseBuffer() {
        if (pooledBuffer && bufferPool != null && batch != null) {
            bufferPool.deallocate(batch);
            batch = null; // Prevent reuse after release
            Logger.debug("Released batch buffer back to pool");
        }
    }
    
    /**
     * Check if this batch uses a pooled buffer
     */
    public boolean isPooledBuffer() {
        return pooledBuffer;
    }
}
