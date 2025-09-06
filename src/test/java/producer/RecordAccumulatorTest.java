package producer;

import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

public class RecordAccumulatorTest {
    @Test
    public void appendTest() throws IOException {
        // Setup
        Headers headers = new Headers();
        headers.add(new Header("Kyoshi", "22".getBytes()));
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "Bob",
                0,
                System.currentTimeMillis(),
                "key",
                "22",
                headers
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Execute
        RecordAccumulator recordAccumulator = new RecordAccumulator(3);
        recordAccumulator.append(serializedData);
        recordAccumulator.printRecord();
    }

    @Test
    public void testMemoryTracking() throws IOException {
        RecordAccumulator accumulator = new RecordAccumulator(10240, 1000, 3); // Small buffer size for testing
        assertEquals(0, accumulator.getTotalBytesUsed());

        // Create test record
        Headers headers = new Headers();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "value", headers
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Append record and check memory usage
        accumulator.append(serializedData);
        assertEquals(serializedData.length, accumulator.getTotalBytesUsed());

        // Try to exceed memory limit
        byte[] largeRecord = new byte[1000];
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            for (int i = 0; i < 10; i++) {
                accumulator.append(largeRecord);
            }
        });
        assertTrue(exception.getMessage().contains("exceed maximum buffer size"));
    }

    @Test
    public void testConcurrentHashMapUsage() throws IOException {
        RecordAccumulator accumulator = new RecordAccumulator(3);
        
        // Create records for different partitions
        Headers headers = new Headers();
        ProducerRecord<String, String> record1 = new ProducerRecord<>("Topic", 0, System.currentTimeMillis(), "key1", "value1", headers);
        ProducerRecord<String, String> record2 = new ProducerRecord<>("Topic", 1, System.currentTimeMillis(), "key2", "value2", headers);
        
        byte[] data1 = ProducerRecordCodec.serialize(record1, String.class, String.class);
        byte[] data2 = ProducerRecordCodec.serialize(record2, String.class, String.class);
        
        accumulator.append(data1);
        accumulator.append(data2);
        
        // Verify we can get batches for different partitions
        assertNotNull(accumulator.getCurrentBatch(0));
        assertNotNull(accumulator.getCurrentBatch(1));
        assertNull(accumulator.getCurrentBatch(2)); // No batch for partition 2 yet
    }

    @Test
    public void testGetters() {
        RecordAccumulator accumulator = new RecordAccumulator(10240, 32 * 1024 * 1024, 5);
        assertEquals(10240, accumulator.getBatchSize());
        assertEquals(32 * 1024 * 1024, accumulator.getMaxBufferSize());
        assertEquals(0, accumulator.getTotalBytesUsed());
    }

    @Test
    public void testDefaultConstructors() {
        RecordAccumulator accumulator1 = new RecordAccumulator(3);
        assertEquals(10240, accumulator1.getBatchSize());
        assertEquals(32 * 1024 * 1024, accumulator1.getMaxBufferSize());

        RecordAccumulator accumulator2 = new RecordAccumulator(16384, 3);
        assertEquals(16384, accumulator2.getBatchSize());
        assertEquals(32 * 1024 * 1024, accumulator2.getMaxBufferSize());
    }
}
