package producer;

import org.tinylog.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe registry for managing batch callbacks and their execution.
 * This class provides centralized callback management with error isolation,
 * ensuring that callback failures do not affect batch processing operations.
 */
public class BatchCallbackRegistry {
    
    private final Map<String, List<BatchCallback>> batchCallbacks = new ConcurrentHashMap<>();
    private final ExecutorService callbackExecutor;
    private final long callbackTimeoutMs;
    private final AtomicInteger activeCallbacks = new AtomicInteger(0);
    
    // Default configuration
    private static final int DEFAULT_CALLBACK_THREADS = 2;
    private static final long DEFAULT_CALLBACK_TIMEOUT_MS = 5000; // 5 seconds
    
    /**
     * Create a new callback registry with default configuration.
     */
    public BatchCallbackRegistry() {
        this(DEFAULT_CALLBACK_THREADS, DEFAULT_CALLBACK_TIMEOUT_MS);
    }
    
    /**
     * Create a new callback registry with custom configuration.
     *
     * @param threadCount Number of threads for callback execution
     * @param callbackTimeoutMs Timeout for individual callback execution
     */
    public BatchCallbackRegistry(int threadCount, long callbackTimeoutMs) {
        this.callbackTimeoutMs = callbackTimeoutMs;
        
        // Create dedicated thread pool for callback execution
        this.callbackExecutor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "batch-callback-" + Thread.currentThread().getId());
            t.setDaemon(true); // Don't prevent JVM shutdown
            return t;
        });
        
        Logger.info("BatchCallbackRegistry initialized with {} threads and {}ms timeout", 
                   threadCount, callbackTimeoutMs);
    }
    
    /**
     * Register a callback for a specific batch.
     * The callback will be notified of batch lifecycle events.
     *
     * @param batchId The unique batch identifier
     * @param callback The callback to register
     */
    public void registerCallback(String batchId, BatchCallback callback) {
        if (batchId == null || callback == null) {
            throw new IllegalArgumentException("batchId and callback cannot be null");
        }
        
        batchCallbacks.computeIfAbsent(batchId, k -> new CopyOnWriteArrayList<>()).add(callback);
        Logger.debug("Registered callback for batch {}: {}", batchId, callback.getClass().getSimpleName());
    }
    
    /**
     * Unregister a specific callback for a batch.
     *
     * @param batchId The unique batch identifier
     * @param callback The callback to remove
     * @return true if the callback was removed, false if it wasn't found
     */
    public boolean unregisterCallback(String batchId, BatchCallback callback) {
        if (batchId == null || callback == null) {
            return false;
        }
        
        List<BatchCallback> callbacks = batchCallbacks.get(batchId);
        if (callbacks != null) {
            boolean removed = callbacks.remove(callback);
            
            // Clean up empty lists to prevent memory leaks
            if (callbacks.isEmpty()) {
                batchCallbacks.remove(batchId);
            }
            
            if (removed) {
                Logger.debug("Unregistered callback for batch {}: {}", batchId, callback.getClass().getSimpleName());
            }
            return removed;
        }
        
        return false;
    }
    
    /**
     * Execute batch creation callbacks for the specified batch.
     *
     * @param batchInfo Information about the created batch
     */
    public void executeBatchCreatedCallbacks(BatchInfo batchInfo) {
        executeCallbacks(batchInfo.getBatchId(), CallbackType.BATCH_CREATED, batchInfo);
    }
    
    /**
     * Execute batch ready callbacks for the specified batch.
     *
     * @param batchInfo Information about the ready batch
     */
    public void executeBatchReadyCallbacks(BatchInfo batchInfo) {
        executeCallbacks(batchInfo.getBatchId(), CallbackType.BATCH_READY, batchInfo);
    }
    
    /**
     * Execute batch sending callbacks for the specified batch.
     *
     * @param batchInfo Information about the batch being sent
     */
    public void executeBatchSendingCallbacks(BatchInfo batchInfo) {
        executeCallbacks(batchInfo.getBatchId(), CallbackType.BATCH_SENDING, batchInfo);
    }
    
    /**
     * Execute batch success callbacks for the specified batch.
     *
     * @param batchInfo Information about the successful batch
     * @param result The broker acknowledgment/result
     */
    public void executeBatchSuccessCallbacks(BatchInfo batchInfo, Object result) {
        executeCallbacks(batchInfo.getBatchId(), CallbackType.BATCH_SUCCESS, batchInfo, result);
        cleanupBatch(batchInfo.getBatchId());
    }
    
    /**
     * Execute batch failure callbacks for the specified batch.
     *
     * @param batchInfo Information about the failed batch
     * @param exception The error that caused the failure
     */
    public void executeBatchFailureCallbacks(BatchInfo batchInfo, Throwable exception) {
        executeCallbacks(batchInfo.getBatchId(), CallbackType.BATCH_FAILURE, batchInfo, exception);
        cleanupBatch(batchInfo.getBatchId());
    }
    
    /**
     * Execute callbacks of the specified type for a batch.
     */
    private void executeCallbacks(String batchId, CallbackType type, Object... args) {
        List<BatchCallback> callbacks = batchCallbacks.get(batchId);
        if (callbacks == null || callbacks.isEmpty()) {
            Logger.debug("No callbacks registered for batch {} and type {}", batchId, type);
            return;
        }
        
        Logger.debug("Executing {} callbacks of type {} for batch {}", callbacks.size(), type, batchId);
        
        // Execute callbacks asynchronously to avoid blocking
        callbackExecutor.submit(() -> {
            activeCallbacks.incrementAndGet();
            try {
                for (BatchCallback callback : callbacks) {
                    executeCallbackSafely(callback, type, batchId, args);
                }
            } finally {
                activeCallbacks.decrementAndGet();
            }
        });
    }
    
    /**
     * Execute a single callback with error isolation and timeout protection.
     */
    private void executeCallbackSafely(BatchCallback callback, CallbackType type, String batchId, Object... args) {
        Future<?> callbackTask = callbackExecutor.submit(() -> {
            try {
                long startTime = System.currentTimeMillis();
                
                switch (type) {
                    case BATCH_CREATED:
                        callback.onBatchCreated((BatchInfo) args[0]);
                        break;
                    case BATCH_READY:
                        callback.onBatchReady((BatchInfo) args[0]);
                        break;
                    case BATCH_SENDING:
                        callback.onBatchSending((BatchInfo) args[0]);
                        break;
                    case BATCH_SUCCESS:
                        callback.onBatchSuccess((BatchInfo) args[0], args[1]);
                        break;
                    case BATCH_FAILURE:
                        callback.onBatchFailure((BatchInfo) args[0], (Throwable) args[1]);
                        break;
                    default:
                        Logger.warn("Unknown callback type: {}", type);
                        return;
                }
                
                long duration = System.currentTimeMillis() - startTime;
                if (duration > 100) { // Log slow callbacks
                    Logger.debug("Slow callback execution for batch {}: {}ms", batchId, duration);
                }
                
            } catch (Exception e) {
                Logger.error("Callback execution failed for batch {} (type {}): {}", 
                           batchId, type, e.getMessage(), e);
                // Continue processing - don't let callback failures affect the system
            }
        });
        
        try {
            callbackTask.get(callbackTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            Logger.warn("Callback execution timed out for batch {} (type {})", batchId, type);
            callbackTask.cancel(true);
        } catch (Exception e) {
            Logger.error("Error waiting for callback completion for batch {} (type {}): {}", 
                       batchId, type, e.getMessage());
        }
    }
    
    /**
     * Remove all callbacks for a batch after it completes or fails.
     * This prevents memory leaks from accumulating callback references.
     */
    private void cleanupBatch(String batchId) {
        List<BatchCallback> removed = batchCallbacks.remove(batchId);
        if (removed != null) {
            Logger.debug("Cleaned up {} callbacks for completed batch {}", removed.size(), batchId);
        }
    }
    
    /**
     * Get the number of batches with registered callbacks.
     *
     * @return Number of batches being tracked
     */
    public int getBatchCount() {
        return batchCallbacks.size();
    }
    
    /**
     * Get the total number of registered callbacks across all batches.
     *
     * @return Total callback count
     */
    public int getTotalCallbackCount() {
        return batchCallbacks.values().stream()
                .mapToInt(List::size)
                .sum();
    }
    
    /**
     * Get the number of currently executing callbacks.
     *
     * @return Active callback count
     */
    public int getActiveCallbackCount() {
        return activeCallbacks.get();
    }
    
    /**
     * Shutdown the callback registry and wait for pending callbacks to complete.
     */
    public void shutdown() {
        Logger.info("Shutting down BatchCallbackRegistry...");
        
        callbackExecutor.shutdown();
        try {
            if (!callbackExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                Logger.warn("Callback executor did not terminate within 10 seconds, forcing shutdown");
                callbackExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            callbackExecutor.shutdownNow();
        }
        
        // Clear all callbacks
        int totalCallbacks = getTotalCallbackCount();
        batchCallbacks.clear();
        
        Logger.info("BatchCallbackRegistry shutdown complete. Cleared {} callbacks", totalCallbacks);
    }
    
    /**
     * Enum for different callback types.
     */
    private enum CallbackType {
        BATCH_CREATED,
        BATCH_READY,
        BATCH_SENDING,
        BATCH_SUCCESS,
        BATCH_FAILURE
    }
}