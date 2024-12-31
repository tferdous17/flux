package producer;

import org.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;

public class RecordBatch {
    private final int maxBatchSizeInBytes;
    private int currBatchSizeInBytes;
    private List<byte[]> batch;

    private static final int DEFAULT_BATCH_SIZE = 10_240; // default batch size: 10 KB = 10,240 bytes

    public RecordBatch() {
        this(DEFAULT_BATCH_SIZE);
    }

    public RecordBatch(int maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.currBatchSizeInBytes = 0;
        this.batch = new ArrayList<>();
    }

    // NOTE: This method may need refactoring once SerializedRecord (Producer) is implemented, but logic remains same
    public boolean append(byte[] serializedRecord) {
        // use a boolean to represent if a record could fit in the current batch or not
        // if not, use the returned `false` value as a signal to create an additional batch
        if (currBatchSizeInBytes + serializedRecord.length > maxBatchSizeInBytes) {
            Logger.warn("Record can not fit in current batch. New one may be necessary");
            return false;
        }
        // record can fit, so add to batch and return true
        batch.add(serializedRecord);
        currBatchSizeInBytes += serializedRecord.length;

        return true;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public int getCurrBatchSizeInBytes() {
        return currBatchSizeInBytes;
    }

    public int getRecordCount() {
        return batch.size();
    }

    public List<byte[]> getBatch() {
        return batch;
    }

    public void printBatchDetails() {
        System.out.println("Number of Records: " + getRecordCount());
        System.out.println("Current Size: " + currBatchSizeInBytes + " bytes");
        System.out.println("Max Batch Size: " + maxBatchSizeInBytes + " bytes\n");
    }
}
