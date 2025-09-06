package producer;

import commons.compression.CompressionType;
import commons.compression.CompressionUtils;
import org.tinylog.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RecordBatch {
    private final int maxBatchSizeInBytes;
    private int currBatchSizeInBytes;
    private int numRecords;
    private ByteBuffer batch;
    private final List<RecordMetadata> recordMetadataList; // Store metadata for each record
    private final List<RecordFuture> recordFutures; // Store futures for each record
    private final long createdTimeMs; // For batch timeout tracking
    private final BufferPool bufferPool; // Optional buffer pool for memory management
    private final boolean pooledBuffer; // Track if this buffer came from a pool
    
    private final String batchId; // Unique identifier for this batch
    private volatile Long sentTimeMs; // Timestamp when batch was sent
    private volatile String topicName; // Primary topic for this batch
    private final CompressionType compressionType; // Compression type for this batch

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
        this(maxBatchSizeInBytes, null, CompressionType.NONE);
    }
    
    public RecordBatch(int maxBatchSizeInBytes, CompressionType compressionType) {
        this(maxBatchSizeInBytes, null, compressionType);
    }
    
    /**
     * Create a RecordBatch with optional BufferPool support and compression
     * @param maxBatchSizeInBytes Maximum size of the batch
     * @param bufferPool Optional buffer pool for memory management
     * @param compressionType Compression type for the batch
     */
    public RecordBatch(int maxBatchSizeInBytes, BufferPool bufferPool, CompressionType compressionType) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.currBatchSizeInBytes = 0;
        this.numRecords = 0;
        this.bufferPool = bufferPool;
        this.recordMetadataList = new ArrayList<>();
        this.recordFutures = new ArrayList<>();
        this.createdTimeMs = System.currentTimeMillis();
        this.compressionType = compressionType != null ? compressionType : CompressionType.NONE;
        
        // Initialize batch fields
        this.batchId = UUID.randomUUID().toString();
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
     * Append a record with metadata to the batch (backward compatibility)
     * @param serializedRecord the serialized record data
     * @param topicName topic name for the record
     * @param partition partition for the record
     * @return true if record fits, false if batch is full
     */
    public boolean append(byte[] serializedRecord, String topicName, int partition) throws IOException {
        return append(serializedRecord, topicName, partition, null);
    }
    
    /**
     * Append a record with metadata to the batch
     * @param serializedRecord the serialized record data
     * @param topicName topic name for the record
     * @param partition partition for the record
     * @param future the future to complete when the record is sent
     * @return true if record fits, false if batch is full
     */
    public boolean append(byte[] serializedRecord, String topicName, int partition, RecordFuture future) throws IOException {
        // Check if record can fit in the current batch
        if (currBatchSizeInBytes + serializedRecord.length > maxBatchSizeInBytes) {
            Logger.warn("Record can not fit in current batch. New one may be necessary");
            return false;
        }
        
        // Set topic name if this is the first record
        if (this.topicName == null && topicName != null) {
            this.topicName = topicName;
        }
        
        // Store metadata and future for this record
        int offsetInBatch = currBatchSizeInBytes;
        RecordMetadata metadata = new RecordMetadata(topicName, partition, serializedRecord.length, offsetInBatch);
        recordMetadataList.add(metadata);
        if (future != null) {
            recordFutures.add(future);
        }
        
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
        System.out.println("Compression: " + compressionType);
        System.out.println("Created: " + createdTimeMs + "ms ago");
        System.out.println("Topic: " + topicName + "\n");
    }
    
    /**
     * Get the compression type for this batch
     */
    public CompressionType getCompressionType() {
        return compressionType;
    }
    
    /**
     * Get the compressed batch data. This compresses the batch buffer
     * using the configured compression type.
     * 
     * @return Compressed batch data as ByteBuffer
     */
    public ByteBuffer getCompressedBatchData() {
        if (batch == null || currBatchSizeInBytes == 0) {
            return ByteBuffer.allocate(0);
        }
        
        // Create a read-only view of the current batch data
        ByteBuffer dataToCompress = batch.duplicate();
        dataToCompress.rewind();
        dataToCompress.limit(currBatchSizeInBytes);
        
        // Compress using the batch's compression type
        return CompressionUtils.compress(dataToCompress, compressionType);
    }
    
    /**
     * Get the unique identifier for this batch.
     *
     * @return The batch ID
     */
    public String getBatchId() {
        return batchId;
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
    
    /**
     * Complete all futures in this batch with success
     * @param baseOffset The base offset assigned by the broker
     */
    public void complete(long baseOffset) {
        for (int i = 0; i < recordFutures.size(); i++) {
            RecordFuture future = recordFutures.get(i);
            RecordMetadata batchMetadata = recordMetadataList.get(i);
            
            // Create final RecordMetadata with the actual offset
            producer.RecordMetadata finalMetadata = new producer.RecordMetadata(
                batchMetadata.topicName,
                batchMetadata.partition,
                baseOffset + i, // Each record gets sequential offset
                System.currentTimeMillis(),
                -1, // serialized key size (unknown)
                batchMetadata.recordLength
            );
            
            future.complete(finalMetadata);
        }
    }
    
    /**
     * Complete all futures in this batch with an exception
     * @param exception The exception that occurred
     */
    public void completeExceptionally(Exception exception) {
        for (RecordFuture future : recordFutures) {
            future.completeExceptionally(exception);
        }
    }
}
