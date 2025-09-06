package producer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

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
        BufferPool pool = new BufferPool(1024, 256);
        
        // Allocate buffer from pool
        java.nio.ByteBuffer buffer = pool.allocate(256, 1000);
        RecordBatch batch = new RecordBatch(buffer);
        
        assertEquals(256, batch.getMaxBatchSizeInBytes());
        assertEquals(256, batch.getInitialCapacity());
        assertSame(buffer, batch.getBuffer());
        
        // Deallocate back to pool
        pool.deallocate(batch.getBuffer(), batch.getInitialCapacity());
        assertEquals(1024, pool.availableMemory());
    }
}