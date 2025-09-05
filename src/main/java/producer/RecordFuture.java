package producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A Future implementation for tracking the result of a record send operation.
 * This provides both synchronous access via Future.get() and asynchronous notification via callbacks.
 */
public class RecordFuture implements Future<RecordMetadata> {
    
    private volatile RecordMetadata result;
    private volatile Exception exception;
    private volatile boolean done = false;
    private final Callback callback;
    private final Object lock = new Object();
    
    /**
     * Create a RecordFuture without a callback
     */
    public RecordFuture() {
        this(null);
    }
    
    /**
     * Create a RecordFuture with an optional callback
     * 
     * @param callback Optional callback to invoke when the record is complete (may be null)
     */
    public RecordFuture(Callback callback) {
        this.callback = callback;
    }
    
    /**
     * Complete this future with successful result
     * 
     * @param metadata The metadata for the successfully sent record
     */
    public void complete(RecordMetadata metadata) {
        synchronized (lock) {
            if (done) {
                return; // Already completed
            }
            this.result = metadata;
            this.done = true;
            lock.notifyAll();
        }
        
        // Execute callback outside of lock to avoid blocking
        if (callback != null) {
            try {
                callback.onCompletion(metadata, null);
            } catch (Exception e) {
                // Don't let callback exceptions affect the future
                // In production, this might be logged
            }
        }
    }
    
    /**
     * Complete this future with an exception
     * 
     * @param exception The exception that occurred during sending
     */
    public void completeExceptionally(Exception exception) {
        synchronized (lock) {
            if (done) {
                return; // Already completed
            }
            this.exception = exception;
            this.done = true;
            lock.notifyAll();
        }
        
        // Execute callback outside of lock
        if (callback != null) {
            try {
                callback.onCompletion(null, exception);
            } catch (Exception e) {
                // Don't let callback exceptions affect the future
            }
        }
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // Record futures cannot be cancelled once created
        return false;
    }
    
    @Override
    public boolean isCancelled() {
        return false;
    }
    
    @Override
    public boolean isDone() {
        return done;
    }
    
    @Override
    public RecordMetadata get() throws InterruptedException, ExecutionException {
        synchronized (lock) {
            while (!done) {
                lock.wait();
            }
            return getResult();
        }
    }
    
    @Override
    public RecordMetadata get(long timeout, TimeUnit unit) 
            throws InterruptedException, ExecutionException, TimeoutException {
        synchronized (lock) {
            if (!done) {
                lock.wait(unit.toMillis(timeout));
                if (!done) {
                    throw new TimeoutException("Timeout waiting for record to complete");
                }
            }
            return getResult();
        }
    }
    
    /**
     * Get the result, throwing ExecutionException if there was an error
     */
    private RecordMetadata getResult() throws ExecutionException {
        if (exception != null) {
            throw new ExecutionException(exception);
        }
        return result;
    }
    
    /**
     * Get the callback associated with this future
     * 
     * @return The callback, or null if no callback was provided
     */
    public Callback getCallback() {
        return callback;
    }
}