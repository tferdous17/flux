package producer;

public class RecordAccumulator {
    static private final int DEFAULT_BATCH_SIZE =  16_384; // 16 bytes

    //TODO: Future work for multiple partitions
//    private Map<TopicPartition, RecordBatch> batches;

    private final int batchSize;

    public RecordAccumulator(){
        this.batchSize = validateBatchSize(DEFAULT_BATCH_SIZE);
    }

    public RecordAccumulator(int batchSize) {
        this.batchSize = validateBatchSize(batchSize);
    }

    public RecordBatch createBatch(int partition, long baseOffset) {

        return new RecordBatch();
    }
    public boolean flush() {
        return false;
    }

    public void append(byte[] serializedRecord) {

    }

    private int validateBatchSize(int batchSize) {
        final int MIN_BATCH_SIZE = 1; // validate its non-zero
        final int MAX_BATCH_SIZE = 1_048_576; // Anything above 1 MB seems unreasonable.

        if (batchSize < MIN_BATCH_SIZE || batchSize > MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    ". Batch size must be between " + MIN_BATCH_SIZE + "-" + MAX_BATCH_SIZE + " bytes."
            );
        }
        return batchSize;
    }
}
