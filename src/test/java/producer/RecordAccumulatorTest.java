package producer;

import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.Test;
import java.io.IOException;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;
import static producer.ProducerTestUtils.*;

public class RecordAccumulatorTest {
    
    @Test
    public void testAppend() throws IOException {
        setupTestTopic("test-topic", 1);
        
        Headers headers = new Headers();
        headers.add(new Header("key", "value".getBytes()));
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test-topic", 0, System.currentTimeMillis(), "key", "value", headers);
        byte[] serializedData = serializeRecord(record);

        RecordAccumulator accumulator = new RecordAccumulator(3);
        accumulator.append(serializedData);
        accumulator.printRecord();
    }

    @Test
    public void testCustomConfiguration() {
        RecordAccumulator accumulator = new RecordAccumulator(
            8192, 3, 200, 15000, 0.8, 16 * 1024 * 1024L);

        assertEquals(8192, accumulator.getBatchSize());
        assertEquals(200, accumulator.getLingerMs());
        assertEquals(15000, accumulator.getBatchTimeoutMs());
        assertEquals(0.8, accumulator.getBatchSizeThreshold(), 0.001);
    }

    @Test
    public void testDefaultValues() {
        RecordAccumulator accumulator = new RecordAccumulator(3);
        
        assertEquals(10240, accumulator.getBatchSize());
        assertEquals(100, accumulator.getLingerMs());
        assertEquals(30000, accumulator.getBatchTimeoutMs());
        assertEquals(0.9, accumulator.getBatchSizeThreshold(), 0.001);
    }

    @Test
    public void testValidConfiguration() {
        assertDoesNotThrow(() -> 
            new RecordAccumulator(1024, 3, 100, 30000, 0.9, 32 * 1024 * 1024L));
    }
    
    @Test
    public void testInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () ->
            new RecordAccumulator(0, 3, 100, 30000, 0.9, 32 * 1024 * 1024L));
        
        assertThrows(IllegalArgumentException.class, () ->
            new RecordAccumulator(1024, 3, 100, 30000, 1.1, 32 * 1024 * 1024L));
    }

    @Test
    public void testBatchThreshold() throws IOException {
        setupTestTopic("threshold-topic", 1);
        
        RecordAccumulator accumulator = new RecordAccumulator(1000, 1, 5000, 10000, 0.5, 32 * 1024 * 1024L);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
            "threshold-topic", 0, System.currentTimeMillis(), "key", "a".repeat(200), new Headers());
        byte[] serializedData = serializeRecord(record);
        
        accumulator.append(serializedData);
        assertTrue(accumulator.getReadyBatches().isEmpty());
        
        accumulator.append(serializedData);
        assertFalse(accumulator.getReadyBatches().isEmpty());
    }

    @Test
    public void testBatchTimeout() throws IOException, InterruptedException {
        setupTestTopic("timeout-topic", 1);
        
        RecordAccumulator accumulator = new RecordAccumulator(1000, 1, 50, 100, 0.9, 32 * 1024 * 1024L);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
            "timeout-topic", 0, System.currentTimeMillis(), "key", "small", new Headers());
        byte[] serializedData = serializeRecord(record);
        
        accumulator.append(serializedData);
        assertTrue(accumulator.getReadyBatches().isEmpty());
        
        Thread.sleep(150);
        assertFalse(accumulator.getReadyBatches().isEmpty());
    }
}
