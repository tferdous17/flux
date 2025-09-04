package producer;

/**
 * Interface for batch completion callbacks in the RecordAccumulator system.
 * Implementations of this interface will receive notifications about batch lifecycle events,
 * allowing for proper resource management, monitoring, and error handling.
 *
 * Callbacks are executed asynchronously on a separate thread pool to avoid blocking
 * batch processing operations. Implementations should be thread-safe and handle
 * exceptions gracefully.
 */
public interface BatchCallback {
    
    /**
     * Called when a batch is successfully sent and acknowledged by the broker.
     * This indicates that all records in the batch have been persisted and 
     * are available for consumption.
     *
     * @param batchInfo Information about the completed batch
     * @param result The broker response/acknowledgment containing offset information
     */
    void onBatchSuccess(BatchInfo batchInfo, Object result);
    
    /**
     * Called when a batch fails to send or is rejected by the broker.
     * This may occur due to network issues, broker errors, or validation failures.
     * After this callback, the batch will not be retried further.
     *
     * @param batchInfo Information about the failed batch
     * @param exception The error that caused the failure
     */
    void onBatchFailure(BatchInfo batchInfo, Throwable exception);
    
    /**
     * Called when a new batch is created and ready to accept records.
     * This is the first lifecycle event for any batch.
     *
     * @param batchInfo Information about the newly created batch
     */
    default void onBatchCreated(BatchInfo batchInfo) {
        // Default empty implementation - override if needed
    }
    
    /**
     * Called when a batch becomes ready for sending.
     * This occurs when the batch reaches its size limit or the linger time expires.
     *
     * @param batchInfo Information about the ready batch
     */
    default void onBatchReady(BatchInfo batchInfo) {
        // Default empty implementation - override if needed
    }
    
    /**
     * Called when a batch is about to be sent to the broker.
     * This occurs just before the network request is initiated.
     *
     * @param batchInfo Information about the batch being sent
     */
    default void onBatchSending(BatchInfo batchInfo) {
        // Default empty implementation - override if needed
    }
}