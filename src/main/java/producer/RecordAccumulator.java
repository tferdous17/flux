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

    public void append(byte[] serializedRecord) throws IOException {
        // Single assumption
        int partition = 0;
        long baseOffset = 0L;

        // Check if current batch exists or is full
        if (currentBatch == null || !currentBatch.append(serializedRecord)) {
            Logger.info("Batch is full or not present. Creating a new batch.");
            currentBatch = createBatch(partition, baseOffset);

            if (!currentBatch.append(serializedRecord)) {
                throw new IllegalStateException("Serialized record cannot fit into a new batch. Check batch size configuration.");
            }
        }

        Logger.info("Record appended successfully.");
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
