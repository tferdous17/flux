package producer;

import commons.utils.CompressionUtil;
import org.tinylog.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RecordBatch {
    private final int maxBatchSizeInBytes;
    private int currBatchSizeInBytes;
    private int numRecords;
    private ByteBuffer batch;
    private final long creationTime;
    private boolean isCompressed;
    private byte[] compressedData;

    private static final int DEFAULT_BATCH_SIZE = 10_240; // default batch size: 10 KB = 10,240 bytes

    public RecordBatch() {
        this(DEFAULT_BATCH_SIZE);
    }

    public RecordBatch(int maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.currBatchSizeInBytes = 0;
        this.numRecords = 0;
        this.batch = ByteBuffer.allocate(maxBatchSizeInBytes);
        this.creationTime = System.currentTimeMillis();
        this.isCompressed = false;
        this.compressedData = null;
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
        return isCompressed;
    }

    /**
     * Compress the batch data using GZIP compression.
     * Only compresses if it reduces the size by at least 5%.
     * @return true if compression was applied, false otherwise
     */
    public boolean compress() throws IOException {
        if (isCompressed || currBatchSizeInBytes == 0) {
            return isCompressed;
        }

        // Get the current data from the buffer
        byte[] originalData = new byte[currBatchSizeInBytes];
        batch.rewind();
        batch.get(originalData, 0, currBatchSizeInBytes);

        // Compress the data
        byte[] compressed = CompressionUtil.gzipCompress(originalData);

        // Only use compression if it's beneficial
        if (CompressionUtil.isCompressionBeneficial(originalData, compressed)) {
            this.compressedData = compressed;
            this.isCompressed = true;
            Logger.info("Batch compressed from {} bytes to {} bytes", 
                       originalData.length, compressed.length);
            return true;
        }

        Logger.info("Compression not beneficial for batch - keeping original data");
        return false;
    }

    /**
     * Get the batch data - either compressed or original
     * @return compressed data if compressed, otherwise original buffer data
     */
    public byte[] getData() throws IOException {
        if (isCompressed) {
            return compressedData;
        }
        
        byte[] data = new byte[currBatchSizeInBytes];
        batch.rewind();
        batch.get(data, 0, currBatchSizeInBytes);
        return data;
    }

    /**
     * Get the size of the batch data (compressed if compression was applied)
     * @return size in bytes
     */
    public int getDataSize() {
        return isCompressed ? compressedData.length : currBatchSizeInBytes;
    }

    public void printBatchDetails() {
        System.out.println("Number of Records: " + getRecordCount());
        System.out.println("Current Size: " + currBatchSizeInBytes + " bytes");
        if (isCompressed) {
            System.out.println("Compressed Size: " + compressedData.length + " bytes");
            double compressionRatio = (double) compressedData.length / currBatchSizeInBytes;
            System.out.println("Compression Ratio: " + String.format("%.2f%%", compressionRatio * 100));
        }
        System.out.println("Max Batch Size: " + maxBatchSizeInBytes + " bytes");
        System.out.println("Compressed: " + isCompressed + "\n");
    }
}
