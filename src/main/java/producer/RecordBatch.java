package producer;

import org.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;

public class RecordBatch {
    private final int maxBatchSizeInBytes;
    private int currBatchSize;
    private int numRecords;
    private List<byte[]> batch;

    public RecordBatch() {
        this(10_000); // default batch size: 10 KB = 100,00 bytes
    }

    public RecordBatch(int maxBatchSizeInBytes) {
        this.maxBatchSizeInBytes = maxBatchSizeInBytes;
        this.currBatchSize = 0;
        this.numRecords = 0;
        this.batch = new ArrayList<>();
    }

    // NOTE: This method may need refactoring once SerializedRecord (Producer) is implemented, but logic remains same
    public boolean append(byte[] serializedRecord) {
        // use a boolean to represent if a record could fit in the current batch or not
        // if not, use the returned `false` value as a signal to create an additional batch
        if (currBatchSize + serializedRecord.length > maxBatchSizeInBytes) {
            Logger.warn("Record can not in current batch. New one may be necessary");
            return false;
        }
        // record can fit, so add to batch and return true
        batch.add(serializedRecord);
        numRecords++;
        currBatchSize += serializedRecord.length;

        return true;
    }

    public int getMaxBatchSizeInBytes() {
        return maxBatchSizeInBytes;
    }

    public int getCurrBatchSize() {
        return currBatchSize;
    }

    public int getNumRecords() {
        return numRecords;
    }

    public List<byte[]> getBatch() {
        return batch;
    }

    public void printBatchDetails() {
        System.out.println("Number of Records: " + numRecords);
        System.out.println("Current Size: " + currBatchSize + " bytes");
        System.out.println("Max Batch Size: " + maxBatchSizeInBytes + " bytes");
    }
}
