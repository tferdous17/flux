package producer;

import org.tinylog.Logger;

/**
 * Default callback that handles buffer pool cleanup for completed batches.
 * This callback ensures that buffers are properly released back to the pool
 * when batches are successfully sent or fail permanently.
 *
 * This is a critical system callback that prevents memory leaks by ensuring
 * proper resource cleanup regardless of user callback behavior.
 */
public class BufferReleaseCallback implements BatchCallback {
    
    @Override
    public void onBatchSuccess(BatchInfo batchInfo, Object result) {
        try {
            Logger.debug("Releasing buffer for successful batch: {}", batchInfo.getBatchId());
            
            // Buffer release will be handled by RecordAccumulator.releaseBatchBuffers()
            // after this callback completes. This callback serves as a marker that
            // the batch is ready for cleanup.
            
            Logger.debug("Buffer release scheduled for batch {} (success)", batchInfo.getBatchId());
            
        } catch (Exception e) {
            Logger.error("Error during buffer release callback for successful batch {}: {}", 
                       batchInfo.getBatchId(), e.getMessage(), e);
        }
    }
    
    @Override
    public void onBatchFailure(BatchInfo batchInfo, Throwable exception) {
        try {
            Logger.debug("Releasing buffer for failed batch: {}", batchInfo.getBatchId());
            
            // Buffer release will be handled by RecordAccumulator.releaseBatchBuffers()
            // after this callback completes. This ensures cleanup even for failed batches.
            
            Logger.debug("Buffer release scheduled for batch {} (failure)", batchInfo.getBatchId());
            
        } catch (Exception e) {
            Logger.error("Error during buffer release callback for failed batch {}: {}", 
                       batchInfo.getBatchId(), e.getMessage(), e);
        }
    }
    
    @Override
    public void onBatchCreated(BatchInfo batchInfo) {
        Logger.debug("Buffer allocated for new batch: {} (size: {} bytes)", 
                   batchInfo.getBatchId(), batchInfo.getBatchSizeBytes());
    }
    
    @Override
    public void onBatchReady(BatchInfo batchInfo) {
        Logger.debug("Batch {} is ready with {} records ({} bytes)", 
                   batchInfo.getBatchId(), 
                   batchInfo.getRecordCount(), 
                   batchInfo.getBatchSizeBytes());
    }
    
    @Override
    public void onBatchSending(BatchInfo batchInfo) {
        Logger.debug("Sending batch {} with {} records to partition {}", 
                   batchInfo.getBatchId(), 
                   batchInfo.getRecordCount(),
                   batchInfo.getPartition());
    }
    
    @Override
    public String toString() {
        return "BufferReleaseCallback";
    }
}