package producer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class BufferPoolTest {

    private BufferPool pool;
    private static final long TOTAL_MEMORY = 1024;
    private static final int POOLABLE_SIZE = 256;

    @BeforeEach
    public void setUp() {
        pool = new BufferPool(TOTAL_MEMORY, POOLABLE_SIZE);
    }

    @Test
    public void testBasicAllocationAndDeallocation() throws InterruptedException {
        ByteBuffer buffer = pool.allocate(POOLABLE_SIZE, 1000);
        assertEquals(POOLABLE_SIZE, buffer.capacity());
        assertEquals(TOTAL_MEMORY - POOLABLE_SIZE, pool.availableMemory());
        
        pool.deallocate(buffer);
        assertEquals(TOTAL_MEMORY, pool.availableMemory());
    }

    @Test
    public void testBufferRecycling() throws InterruptedException {
        ByteBuffer buffer1 = pool.allocate(POOLABLE_SIZE, 1000);
        pool.deallocate(buffer1);
        
        ByteBuffer buffer2 = pool.allocate(POOLABLE_SIZE, 1000);
        assertSame(buffer1, buffer2);
    }

    @Test
    public void testMemoryExhaustion() throws InterruptedException {
        // Fill up most of the pool, leaving some room for JVM overhead
        // Allocate 3 buffers instead of 4 to avoid OOM (768 bytes of 1024)
        pool.allocate(POOLABLE_SIZE, 1000);
        pool.allocate(POOLABLE_SIZE, 1000);
        pool.allocate(POOLABLE_SIZE, 1000);
        
        // Should have 256 bytes left (1 buffer worth)
        assertEquals(POOLABLE_SIZE, pool.availableMemory());
        
        // Now allocate the last buffer to exhaust the pool
        pool.allocate(POOLABLE_SIZE, 1000);
        
        assertEquals(0, pool.availableMemory());
        assertThrows(IllegalStateException.class, () -> pool.allocate(POOLABLE_SIZE, 100));
    }

    @Test
    public void testFairness() throws InterruptedException {
        // Fill up the pool
        ByteBuffer buffer1 = pool.allocate(POOLABLE_SIZE, 1000);
        pool.allocate(POOLABLE_SIZE, 1000);
        pool.allocate(POOLABLE_SIZE, 1000);
        pool.allocate(POOLABLE_SIZE, 1000);

        AtomicBoolean thread1First = new AtomicBoolean(false);
        CountDownLatch threadsReady = new CountDownLatch(2);
        CountDownLatch testComplete = new CountDownLatch(1);

        Thread thread1 = new Thread(() -> {
            try {
                threadsReady.countDown();
                threadsReady.await();
                pool.allocate(POOLABLE_SIZE, 2000);
                thread1First.set(true);
                testComplete.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        Thread thread2 = new Thread(() -> {
            try {
                threadsReady.countDown();
                threadsReady.await();
                Thread.sleep(10); // Ensure thread1 waits first
                pool.allocate(POOLABLE_SIZE, 5000); // Increased timeout to avoid test flakiness
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        thread1.start();
        thread2.start();
        Thread.sleep(50);
        
        pool.deallocate(buffer1); // Should go to thread1 first
        assertTrue(testComplete.await(1, TimeUnit.SECONDS));
        assertTrue(thread1First.get());
    }

    @Test
    public void testMetrics() throws InterruptedException {
        assertEquals(TOTAL_MEMORY, pool.totalMemory());
        assertEquals(POOLABLE_SIZE, pool.poolableSize());
        assertEquals(TOTAL_MEMORY, pool.availableMemory());
        assertEquals(0, pool.queued());
        
        // Fill up pool and check queued metric
        pool.allocate(POOLABLE_SIZE, 1000);
        pool.allocate(POOLABLE_SIZE, 1000);
        pool.allocate(POOLABLE_SIZE, 1000);
        ByteBuffer lastBuffer = pool.allocate(POOLABLE_SIZE, 1000);

        CountDownLatch threadReady = new CountDownLatch(1);
        Thread waitingThread = new Thread(() -> {
            try {
                threadReady.countDown();
                pool.allocate(POOLABLE_SIZE, 2000);
            } catch (Exception e) {
                // Expected
            }
        });

        waitingThread.start();
        threadReady.await();
        Thread.sleep(50);
        
        assertEquals(1, pool.queued());
        pool.deallocate(lastBuffer);
        waitingThread.join();
        assertEquals(0, pool.queued());
    }
}