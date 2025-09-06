package producer;

import commons.CompressionType;
import org.tinylog.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RecordBatch {
    private final int maxBatchSizeInBytes;
    private int currBatchSizeInBytes;
    private int numRecords;
    private ByteBuffer batch;
    private final long creationTime;
    private CompressionType compressionType;
    private byte[] compressedData;
    private final int initialCapacity; // Track initial capacity for BufferPool deallocation
    private int retryCount;

    private static final int DEFAULT_BATCH_SIZE = 10_240; // default batch size: 10 KB = 10,240 bytes

    public RecordBatch() {
        this(DEFAULT_BATCH_SIZE, CompressionType.NONE);
    }

    public RecordBatch(int maxBatchSizeInBytes) {
        this(maxBatchSizeInBytes, CompressionType.NONE);
    }

    public RecordBatch(int maxBatchSizeInBytes, CompressionType compressionType) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.currBatchSizeInBytes = 0;
        this.numRecords = 0;
        this.batch = ByteBuffer.allocate(maxBatchSizeInBytes);
        this.creationTime = System.currentTimeMillis();
        this.compressionType = compressionType != null ? compressionType : CompressionType.NONE;
        this.compressedData = null;
        this.initialCapacity = maxBatchSizeInBytes;
        this.retryCount = 0;
    }

    /**
     * Create a RecordBatch with a pre-allocated buffer from BufferPool
     * 
     * @param buffer Pre-allocated ByteBuffer from BufferPool
     */
    public RecordBatch(ByteBuffer buffer) {
        this(buffer, CompressionType.NONE);
    }

    public RecordBatch(ByteBuffer buffer, CompressionType compressionType) {
        this.maxBatchSizeInBytes = buffer.capacity();
        this.currBatchSizeInBytes = 0;
        this.numRecords = 0;
        this.batch = buffer;
        this.creationTime = System.currentTimeMillis();
        this.compressionType = compressionType != null ? compressionType : CompressionType.NONE;
        this.compressedData = null;
        this.initialCapacity = buffer.capacity();
        this.retryCount = 0;
    }

    // NOTE: This method may need refactoring once SerializedRecord (Producer) is implemented, but logic remains same
    public boolean append(byte[] serializedRecord) throws IOException {
        // use a boolean to represent if a record could fit in the current batch or not
        // if not, use the returned `false` value as a signal to create an additional batch
        if (currBatchSizeInBytes + serializedRecord.length > maxBatchSizeInBytes) {
            Logger.warn("Record can not fit in current batch. New one may be necessary");
            return false;
        }
        // record can fit, so add to batch and return true
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

    public long getCreationTime() {
        return creationTime;
    }

    public boolean isFull() {
        return currBatchSizeInBytes >= maxBatchSizeInBytes;
    }

    public long getAge() {
        return System.currentTimeMillis() - creationTime;
    }

    public boolean isCompressed() {
        return compressedData != null;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    /**
     * Compress the batch data using the configured compression type.
     * 
     * @return true if compression was applied, false otherwise
     */
    public boolean compress() throws IOException {
        if (compressedData != null || currBatchSizeInBytes == 0 || compressionType == CompressionType.NONE) {
            return compressedData != null;
        }

        // Get the current data from the buffer
        byte[] originalData = new byte[currBatchSizeInBytes];
        batch.rewind();
        batch.get(originalData, 0, currBatchSizeInBytes);

        // Compress the data using the configured compression type
        byte[] compressed = compressionType.compress(originalData);
        this.compressedData = compressed;

        Logger.info("Batch compressed using {} from {} bytes to {} bytes",
                compressionType.getName(), originalData.length, compressed.length);
        return true;
    }

    /**
     * Get the batch data - either compressed or original
     * 
     * @return compressed data if compressed, otherwise original buffer data
     */
    public byte[] getData() throws IOException {
        if (compressedData != null) {
            return compressedData;
        }

        byte[] data = new byte[currBatchSizeInBytes];
        batch.rewind();
        batch.get(data, 0, currBatchSizeInBytes);
        return data;
    }

    /**
     * Get the size of the batch data (compressed if compression was applied)
     * 
     * @return size in bytes
     */
    public int getDataSize() {
        return compressedData != null ? compressedData.length : currBatchSizeInBytes;
    }

    public void printBatchDetails() {
        System.out.println("Number of Records: " + getRecordCount());
        System.out.println("Current Size: " + currBatchSizeInBytes + " bytes");
        if (compressedData != null) {
            System.out.println("Compressed Size: " + compressedData.length + " bytes");
            double compressionRatio = (double) compressedData.length / currBatchSizeInBytes;
            System.out.println("Compression Ratio: " + String.format("%.2f%%", compressionRatio * 100));
        }
        System.out.println("Max Batch Size: " + maxBatchSizeInBytes + " bytes");
        System.out.println("Compression Type: " + compressionType.getName() + "\n");
    }

    /**
     * Get the initial capacity for BufferPool deallocation
     * 
     * @return initial capacity in bytes
     */
    public int getInitialCapacity() {
        return initialCapacity;
    }

    /**
     * Get the underlying buffer for BufferPool deallocation
     * 
     * @return ByteBuffer instance
     */
    public ByteBuffer getBuffer() {
        return batch;
    }
    
    /**
     * Get the estimated size of this batch in bytes
     * @return Estimated size in bytes (current position in the buffer)
     */
    public int estimatedSizeInBytes() {
        return batch.position();
    }
    
    /**
     * Get the retry count for this batch
     * 
     * @return retry count
     */
    public int getRetryCount() {
        return retryCount;
    }
    
    /**
     * Increment the retry count for this batch
     */
    public void incrementRetryCount() {
        retryCount++;
    }
}
