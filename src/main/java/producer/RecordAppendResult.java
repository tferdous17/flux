package producer;

/**
 * The result of appending a record to the RecordAccumulator.
 * This class provides information about the append operation and a future
 * to track the record's eventual completion.
 * 
 * This matches the pattern used by Kafka's RecordAccumulator.
 */
public class RecordAppendResult {
    
    public final RecordFuture future;
    public final boolean batchIsFull;
    public final boolean newBatchCreated;
    
    /**
     * Create a RecordAppendResult
     * 
     * @param future The future that will be completed when the record is sent
     * @param batchIsFull Whether the batch is now full after this append
     * @param newBatchCreated Whether a new batch was created for this append
     */
    public RecordAppendResult(RecordFuture future, boolean batchIsFull, boolean newBatchCreated) {
        this.future = future;
        this.batchIsFull = batchIsFull;
        this.newBatchCreated = newBatchCreated;
    }
    
    @Override
    public String toString() {
        return String.format("RecordAppendResult(batchIsFull=%s, newBatchCreated=%s)", 
                           batchIsFull, newBatchCreated);
    }
}