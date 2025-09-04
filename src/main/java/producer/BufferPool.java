package producer;

import org.tinylog.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool of ByteBuffers that can be reused to reduce allocation overhead.
 * This implementation is thread-safe and manages memory limits.
 */
public class BufferPool {
    private final long totalMemory;
    private long availableMemory;
    private final int poolableSize;
    private final Deque<ByteBuffer> free;
    private final Lock lock;
    private final Condition moreMemory;
    private boolean closed;
    private final long maxBlockTimeMs;
    
    // Metrics
    private long allocationsCount = 0;
    private long deallocationsCount = 0;
    private long totalWaitTimeNs = 0;
    
    /**
     * Create a new buffer pool
     * @param totalMemory The maximum amount of memory this pool can allocate
     * @param poolableSize The size of buffers to pool
     * @param maxBlockTimeMs Maximum time to block waiting for memory
     */
    public BufferPool(long totalMemory, int poolableSize, long maxBlockTimeMs) {
        this.totalMemory = totalMemory;
        this.availableMemory = totalMemory;
        this.poolableSize = poolableSize;
        this.free = new ArrayDeque<>();
        this.lock = new ReentrantLock();
        this.moreMemory = lock.newCondition();
        this.closed = false;
        this.maxBlockTimeMs = maxBlockTimeMs;
        
        Logger.info("BufferPool initialized - Total Memory: {}MB, Buffer Size: {}KB, Max Block Time: {}ms",
                totalMemory / (1024 * 1024), poolableSize / 1024, maxBlockTimeMs);
    }
    
    /**
     * Default constructor with sensible defaults
     */
    public BufferPool() {
        this(32 * 1024 * 1024L, 16 * 1024, 1000); // 32MB total, 16KB buffers, 1s max wait
    }
    
    /**
     * Allocate a buffer of the given size.
     * 
     * @param size The size of buffer to allocate
     * @return The allocated buffer
     * @throws IllegalArgumentException if size is invalid
     * @throws IllegalStateException if pool is closed
     * @throws InterruptedException if interrupted while waiting
     */
    public ByteBuffer allocate(int size) throws InterruptedException {
        if (size <= 0 || size > totalMemory) {
            throw new IllegalArgumentException("Invalid buffer size: " + size);
        }
        
        long startWait = System.nanoTime();
        lock.lock();
        try {
            if (closed) {
                throw new IllegalStateException("Buffer pool is closed");
            }
            
            // Try to reuse a buffer from the free pool if size matches
            if (size == poolableSize && !free.isEmpty()) {
                allocationsCount++;
                return free.poll();
            }
            
            // Wait for memory if needed
            long remainingTimeMs = maxBlockTimeMs;
            while (availableMemory < size) {
                if (remainingTimeMs <= 0) {
                    throw new IllegalStateException("Failed to allocate buffer of size " + size + 
                            " within timeout. Available memory: " + availableMemory);
                }
                
                long startAwait = System.currentTimeMillis();
                if (!moreMemory.await(remainingTimeMs, TimeUnit.MILLISECONDS)) {
                    throw new IllegalStateException("Timeout waiting for buffer allocation");
                }
                
                if (closed) {
                    throw new IllegalStateException("Buffer pool is closed");
                }
                
                remainingTimeMs -= (System.currentTimeMillis() - startAwait);
            }
            
            // Allocate new buffer
            availableMemory -= size;
            allocationsCount++;
            totalWaitTimeNs += (System.nanoTime() - startWait);
            
            return ByteBuffer.allocate(size);
            
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Return a buffer to the pool.
     * 
     * @param buffer The buffer to return
     */
    public void deallocate(ByteBuffer buffer) {
        if (buffer == null) {
            return;
        }
        
        lock.lock();
        try {
            int size = buffer.capacity();
            buffer.clear(); // Reset the buffer for reuse
            
            // If it's a poolable size buffer, add it back to the free pool
            if (size == poolableSize && !closed) {
                free.add(buffer);
            }
            
            // Return the memory to available pool
            availableMemory += size;
            deallocationsCount++;
            
            // Wake up a thread waiting for memory
            moreMemory.signal();
            
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Check if we're under memory pressure
     * 
     * @return true if available memory is less than 10% of total
     */
    public boolean isUnderMemoryPressure() {
        lock.lock();
        try {
            // Consider under pressure if less than 10% of memory is available
            return availableMemory < (totalMemory * 0.1);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get the amount of memory available for allocation
     */
    public long availableMemory() {
        lock.lock();
        try {
            return availableMemory;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get the total memory managed by this pool
     */
    public long totalMemory() {
        return totalMemory;
    }
    
    /**
     * Get the number of buffers currently in the free pool
     */
    public int freeBufferCount() {
        lock.lock();
        try {
            return free.size();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Close the buffer pool and release all resources
     */
    public void close() {
        lock.lock();
        try {
            if (!closed) {
                closed = true;
                free.clear();
                moreMemory.signalAll(); // Wake up any waiting threads
                Logger.info("BufferPool closed - Allocations: {}, Deallocations: {}, Avg Wait: {}ms",
                        allocationsCount, deallocationsCount, 
                        allocationsCount > 0 ? (totalWaitTimeNs / allocationsCount) / 1_000_000 : 0);
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Get pool statistics for monitoring
     */
    public PoolStats getStats() {
        lock.lock();
        try {
            return new PoolStats(
                    totalMemory,
                    availableMemory,
                    free.size(),
                    allocationsCount,
                    deallocationsCount,
                    allocationsCount > 0 ? totalWaitTimeNs / allocationsCount : 0
            );
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Statistics for the buffer pool
     */
    public static class PoolStats {
        public final long totalMemory;
        public final long availableMemory;
        public final int freeBuffers;
        public final long allocations;
        public final long deallocations;
        public final long avgWaitTimeNs;
        
        public PoolStats(long totalMemory, long availableMemory, int freeBuffers,
                         long allocations, long deallocations, long avgWaitTimeNs) {
            this.totalMemory = totalMemory;
            this.availableMemory = availableMemory;
            this.freeBuffers = freeBuffers;
            this.allocations = allocations;
            this.deallocations = deallocations;
            this.avgWaitTimeNs = avgWaitTimeNs;
        }
        
        @Override
        public String toString() {
            return String.format("PoolStats[memory=%d/%d, freeBuffers=%d, allocations=%d, deallocations=%d, avgWaitMs=%.2f]",
                    availableMemory, totalMemory, freeBuffers, allocations, deallocations, avgWaitTimeNs / 1_000_000.0);
        }
    }
}