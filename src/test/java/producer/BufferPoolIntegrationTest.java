package producer;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

public class BufferPoolIntegrationTest {

    @Test
    public void testRecordAccumulatorWithBufferPool() {
        Properties props = new Properties();
        props.setProperty("buffer.memory", "32768"); // 32KB
        props.setProperty("batch.size", "16384"); // 16KB
        props.setProperty("max.block.ms", "5000"); // 5 seconds
        
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 4);
        
        // Verify BufferPool is properly initialized
        BufferPool bufferPool = accumulator.getBufferPool();
        assertNotNull(bufferPool);
        assertEquals(32768L, bufferPool.totalMemory());
        assertEquals(16384, bufferPool.poolableSize());
        assertEquals(32768L, bufferPool.availableMemory());
        assertEquals(0, bufferPool.queued());
    }

    @Test
    public void testProducerConfigBufferSettings() {
        Properties props = new Properties();
        props.setProperty("buffer.memory", "65536"); // 64KB
        props.setProperty("max.block.ms", "10000"); // 10 seconds
        
        ProducerConfig config = new ProducerConfig(props);
        
        assertEquals(65536L, config.getBufferMemory());
        assertEquals(10000L, config.getMaxBlockMs());
        // Default values should still work
        assertEquals(16384, config.getBatchSize());
        assertEquals(100, config.getLingerMs());
    }
    
    @Test
    public void testDefaultProducerConfigBufferSettings() {
        ProducerConfig config = new ProducerConfig();
        
        // Check defaults
        assertEquals(33554432L, config.getBufferMemory()); // 32MB
        assertEquals(60000L, config.getMaxBlockMs()); // 60 seconds
        assertEquals(16384, config.getBatchSize()); // 16KB
    }

    @Test
    public void testRecordBatchWithBufferPool() throws InterruptedException {
        // Use Mockito mock to avoid real memory allocation
        BufferPool pool = mock(BufferPool.class);
        ByteBuffer mockBuffer = ByteBuffer.allocate(100);
        
        // Configure mock behavior
        when(pool.allocate(anyInt(), anyLong())).thenReturn(mockBuffer);
        when(pool.availableMemory()).thenReturn(1024L);
        when(pool.totalMemory()).thenReturn(1024L);
        when(pool.poolableSize()).thenReturn(256);
        
        // Allocate buffer from pool
        ByteBuffer buffer = pool.allocate(256, 1000);
        RecordBatch batch = new RecordBatch(buffer);
        
        // Mock returns buffer of size 100
        assertEquals(100, batch.getMaxBatchSizeInBytes());
        assertEquals(100, batch.getInitialCapacity());
        assertSame(buffer, batch.getBuffer());
        
        // Deallocate back to pool
        pool.deallocate(batch.getBuffer(), batch.getInitialCapacity());
        assertEquals(1024L, pool.availableMemory());
        
        // Verify mock interactions
        verify(pool).allocate(256, 1000);
        verify(pool).deallocate(buffer, 100);
    }
}