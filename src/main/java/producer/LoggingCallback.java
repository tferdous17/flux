package producer;

import org.tinylog.Logger;

/**
 * Default callback that provides comprehensive logging for batch lifecycle events.
 * This callback helps with debugging, monitoring, and understanding system behavior
 * by logging key information about batch progression.
 *
 * All log messages from this callback use appropriate log levels:
 * - DEBUG: Detailed lifecycle events for debugging
 * - INFO: Important batch completions and metrics
 * - WARN: Slow operations or potential issues
 * - ERROR: Failures that should be investigated
 */
public class LoggingCallback implements BatchCallback {
    
    private static final long SLOW_BATCH_THRESHOLD_MS = 10000; // 10 seconds
    private static final int LARGE_BATCH_THRESHOLD = 1000; // 1000 records
    
    @Override
    public void onBatchCreated(BatchInfo batchInfo) {
        Logger.debug("BATCH_CREATED: {} | partition={} | topic={}", 
                   batchInfo.getBatchId(), 
                   batchInfo.getPartition(),
                   batchInfo.getTopicName());
    }
    
    @Override
    public void onBatchReady(BatchInfo batchInfo) {
        long ageMs = batchInfo.getAgeMs();
        
        if (ageMs > SLOW_BATCH_THRESHOLD_MS) {
            Logger.warn("BATCH_READY: {} | partition={} | records={} | size={}B | age={}ms (SLOW)", 
                       batchInfo.getBatchId(),
                       batchInfo.getPartition(),
                       batchInfo.getRecordCount(),
                       batchInfo.getBatchSizeBytes(),
                       ageMs);
        } else {
            Logger.debug("BATCH_READY: {} | partition={} | records={} | size={}B | age={}ms", 
                        batchInfo.getBatchId(),
                        batchInfo.getPartition(),
                        batchInfo.getRecordCount(),
                        batchInfo.getBatchSizeBytes(),
                        ageMs);
        }
    }
    
    @Override
    public void onBatchSending(BatchInfo batchInfo) {
        if (batchInfo.getRecordCount() > LARGE_BATCH_THRESHOLD) {
            Logger.info("BATCH_SENDING: {} | partition={} | records={} | size={}B (LARGE BATCH)", 
                       batchInfo.getBatchId(),
                       batchInfo.getPartition(),
                       batchInfo.getRecordCount(),
                       batchInfo.getBatchSizeBytes());
        } else {
            Logger.debug("BATCH_SENDING: {} | partition={} | records={} | size={}B", 
                        batchInfo.getBatchId(),
                        batchInfo.getPartition(),
                        batchInfo.getRecordCount(),
                        batchInfo.getBatchSizeBytes());
        }
    }
    
    @Override
    public void onBatchSuccess(BatchInfo batchInfo, Object result) {
        long latencyMs = batchInfo.getSendLatencyMs();
        String resultInfo = formatResult(result);
        
        if (latencyMs > SLOW_BATCH_THRESHOLD_MS) {
            Logger.warn("BATCH_SUCCESS: {} | partition={} | records={} | latency={}ms (SLOW) | result={}", 
                       batchInfo.getBatchId(),
                       batchInfo.getPartition(),
                       batchInfo.getRecordCount(),
                       latencyMs,
                       resultInfo);
        } else {
            Logger.info("BATCH_SUCCESS: {} | partition={} | records={} | latency={}ms | result={}", 
                       batchInfo.getBatchId(),
                       batchInfo.getPartition(),
                       batchInfo.getRecordCount(),
                       latencyMs,
                       resultInfo);
        }
    }
    
    @Override
    public void onBatchFailure(BatchInfo batchInfo, Throwable exception) {
        long ageMs = batchInfo.getAgeMs();
        String errorMsg = exception != null ? exception.getMessage() : "Unknown error";
        String errorType = exception != null ? exception.getClass().getSimpleName() : "UnknownException";
        
        Logger.error("BATCH_FAILURE: {} | partition={} | records={} | age={}ms | error={}:{}", 
                    batchInfo.getBatchId(),
                    batchInfo.getPartition(),
                    batchInfo.getRecordCount(),
                    ageMs,
                    errorType,
                    errorMsg);
        
        // Log additional details for debugging if available
        if (exception != null && Logger.isDebugEnabled()) {
            Logger.debug("BATCH_FAILURE_DETAILS: {} | stackTrace:", batchInfo.getBatchId(), exception);
        }
    }
    
    /**
     * Format the result object for logging.
     * This handles different types of results that might be returned from the broker.
     */
    private String formatResult(Object result) {
        if (result == null) {
            return "null";
        }
        
        try {
            // Handle common result types
            String resultStr = result.toString();
            
            // Truncate very long results for readability
            if (resultStr.length() > 200) {
                return resultStr.substring(0, 200) + "...";
            }
            
            return resultStr;
            
        } catch (Exception e) {
            return result.getClass().getSimpleName() + "@" + Integer.toHexString(result.hashCode());
        }
    }
    
    @Override
    public String toString() {
        return "LoggingCallback";
    }
}