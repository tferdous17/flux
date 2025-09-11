package producer;

import org.tinylog.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pool of ByteBuffers for efficient memory management in the producer.
 * 
 * This class manages a pool of ByteBuffers with a fixed memory limit. Buffers
 * matching
 * the poolable size are recycled for efficiency, while other sizes are
 * allocated directly.
 * 
 * Memory allocation follows a fair queuing policy - the longest waiting thread
 * gets
 * memory first when it becomes available.
 * 
 * Based on Kafka's BufferPool implementation.
 */
public class BufferPool {
    private final long totalMemory;
    private final int poolableSize;
    private final ReentrantLock lock;
    private final Deque<ByteBuffer> free;
    private final Deque<Condition> waiters;
    private long nonPooledAvailableMemory;

    /**
     * Create a new buffer pool
     * 
     * @param memory       The maximum amount of memory that this buffer pool can
     *                     allocate
     * @param poolableSize The buffer size to cache in the free list rather than
     *                     deallocating
     */
    public BufferPool(long memory, int poolableSize) {
        this.poolableSize = poolableSize;
        this.lock = new ReentrantLock();
        this.free = new ArrayDeque<>();
        this.waiters = new ArrayDeque<>();
        this.totalMemory = memory;
        this.nonPooledAvailableMemory = memory;
    }

    /**
     * Allocate a buffer of the given size. This method may block if there is not
     * sufficient memory and the buffer pool is configured to block on exhaustion.
     * 
     * @param size             The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer
     *                         memory to be available
     * @return The buffer
     * @throws IllegalArgumentException if size is larger than the total memory
     *                                  controlled by the pool
     * @throws InterruptedException     if the thread is interrupted while blocked
     * @throws IllegalStateException    if buffer memory could not be allocated
     *                                  within the timeout
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory) {
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");
        }

        ByteBuffer buffer = null;
        this.lock.lock();
        try {
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !this.free.isEmpty()) {
                return this.free.pollFirst();
            }

            // now check if the request fits in the free memory
            int freeListSize = freeSize();
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                freeUp(size);
                this.nonPooledAvailableMemory -= size;
            } else {
                // we are out of memory and will have to block
                int accumulated = 0;
                Condition moreMemory = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = maxTimeToBlockMs * 1_000_000L; // convert to nanoseconds
                    this.waiters.addLast(moreMemory);

                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        long startWaitNs = System.nanoTime();
                        long timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs,
                                    java.util.concurrent.TimeUnit.NANOSECONDS);
                            timeNs = System.nanoTime() - startWaitNs;
                        } catch (InterruptedException e) {
                            this.waiters.remove(moreMemory);
                            throw e;
                        }

                        if (waitingTimeElapsed) {
                            this.waiters.remove(moreMemory);
                            throw new IllegalStateException(
                                    "Failed to allocate memory within the configured max blocking time "
                                            + maxTimeToBlockMs + " ms. Total memory: " + totalMemory() + " bytes.");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from what was freed up
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, so figure out how much we can use from the
                            // free list
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available
                    // memory
                    this.nonPooledAvailableMemory += accumulated;
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty()) {
                    this.waiters.peekFirst().signal();
                }
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }

        if (buffer == null) {
            return safeAllocateByteBuffer(size);
        } else {
            return buffer;
        }
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory
     * for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size) {
            this.nonPooledAvailableMemory += this.poolableSize;
            this.free.pollLast();
        }
    }

    /**
     * Return buffers to the pool. If the buffer is of the poolable size add it to
     * the free list,
     * otherwise just mark the memory as free.
     *
     * @param buffer buffer to return
     * @param size   the size of the buffer to mark as deallocated, note that this
     *               maybe smaller than buffer.capacity
     *               since the buffer may re-allocate itself during in-place
     *               compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                this.nonPooledAvailableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null) {
                moreMem.signal();
            }
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize();
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing
    protected int freeSize() {
        return this.free.size() * this.poolableSize;
    }

    /**
     * Get the number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    /**
     * Allocate a buffer outside of the lock, we do this to avoid
     * potential deadlock with the produce request
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            ByteBuffer buffer = ByteBuffer.allocate(size);
            error = false;
            return buffer;
        } catch (OutOfMemoryError e) {
            if (error) {
                this.lock.lock();
                try {
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty()) {
                        this.waiters.peekFirst().signal();
                    }
                } finally {
                    this.lock.unlock();
                }
            }
            throw new OutOfMemoryError("Failed to allocate " + size + " bytes from the buffer pool (allocated: "
                    + (this.totalMemory - availableMemory()) + " bytes)");
        }
    }
}