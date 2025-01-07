package producer;

import org.tinylog.Logger;

import java.io.IOException;

public class RecordAccumulator {
    static private final int DEFAULT_BATCH_SIZE = 10_240; // 10 KB

    private final int batchSize;
    private RecordBatch currentBatch; // Active batch for the single partition

    public RecordAccumulator() {
        this.batchSize = validateBatchSize(DEFAULT_BATCH_SIZE);
    }

    public RecordAccumulator(int batchSize) {
        this.batchSize = validateBatchSize(batchSize);
    }

    public RecordBatch createBatch(int partition, long baseOffset) {
        Logger.info("Creating new batch for partition " + partition + " with baseOffset " + baseOffset);
        return new RecordBatch(batchSize);
    }

    //TODO: Broker class missing
    public boolean flush() {
        Logger.info("Flushing the batch to the broker (Stubbed out)");
        return true;
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
        // Single partition assumption
        int partition = 0;
        int baseOffset = 0;
        try {
            if (currentBatch == null || !currentBatch.append(serializedRecord)) {
                if (currentBatch != null) { // Case 1B
                    Logger.info("Batch is full. Flushing current batch.");
                    flush(); // TODO: Missing implementation
                }
                Logger.info("Creating a new batch for partition " + partition + ".");
                currentBatch = createBatch(partition, baseOffset); // Step 3
                if (!currentBatch.append(serializedRecord)) {
                    throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
                }
            }
            Logger.info("Record appended successfully.");
        } catch (Exception e) {
            Logger.error("Failed to append record: " + e.getMessage(), e);
            throw e;
        }
    }


    public RecordBatch getCurrentBatch() {
        return currentBatch;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void printRecord() {
        System.out.println("Batch Size: " + getBatchSize());
        System.out.println("Current Batch:");

        getCurrentBatch().printBatchDetails();
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
}
