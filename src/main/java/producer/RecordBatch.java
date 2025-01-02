package producer;

import org.tinylog.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RecordBatch {
    private final int maxBatchSizeInBytes;
    private int currBatchSizeInBytes;
    private int numRecords;
    private ByteBuffer batch;

    private static final int DEFAULT_BATCH_SIZE = 10_240; // default batch size: 10 KB = 10,240 bytes

    public RecordBatch() {
        this(DEFAULT_BATCH_SIZE);
    }

    public RecordBatch(int maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.currBatchSizeInBytes = 0;
        this.numRecords = 0;
        this.batch = ByteBuffer.allocate(DEFAULT_BATCH_SIZE);
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

    public void printBatchDetails() {
        System.out.println("Number of Records: " + getRecordCount());
        System.out.println("Current Size: " + currBatchSizeInBytes + " bytes");
        System.out.println("Max Batch Size: " + maxBatchSizeInBytes + " bytes\n");
    }
}
