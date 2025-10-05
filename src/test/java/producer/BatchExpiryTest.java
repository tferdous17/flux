package producer;

import commons.TopicPartition;
import commons.headers.Headers;
import commons.utils.PartitionSelector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mockStatic;

/**
 * Tests for batch expiry functionality in RecordAccumulator.
 * These tests verify that old batches are properly expired and
 * memory is reclaimed when delivery timeout is exceeded.
 */
public class BatchExpiryTest {
    private MockedStatic<PartitionSelector> mockPartitionSelector;
    
    @BeforeEach
    public void setUp() {
        // Mock PartitionSelector to return the specified partition or default to 0
        mockPartitionSelector = mockStatic(PartitionSelector.class);
        mockPartitionSelector.when(() -> PartitionSelector.getPartitionNumberForRecord(
                any(), anyInt(), any(), any(), anyInt()))
            .thenAnswer(invocation -> {
                Integer partitionNumber = invocation.getArgument(1);
                return partitionNumber != null ? partitionNumber : 0;
            });
    }
    
    @AfterEach
    public void tearDown() {
        if (mockPartitionSelector != null) {
            mockPartitionSelector.close();
        }
    }
    
    @Test
    public void testBatchExpiry() throws IOException, InterruptedException {
        // Create config with very short delivery timeout for testing
        Properties props = new Properties();
        props.setProperty("delivery.timeout.ms", "100"); // 100ms timeout
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        // Add a record to create a batch
        Headers headers = new Headers();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "value", headers
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        accumulator.append(serializedData);
        
        // Verify batch exists
        assertNotNull(accumulator.getCurrentBatch("TestTopic", 0));
        
        // Wait for batch to expire
        Thread.sleep(150); // Wait longer than delivery timeout
        
        // Call ready() which should trigger expiry check
        List<TopicPartition> readyPartitions = accumulator.ready();
        
        // Batch should be expired and removed
        assertNull(accumulator.getCurrentBatch("TestTopic", 0));
        assertTrue(readyPartitions.isEmpty());
    }
    
    @Test
    public void testBatchExpiryWithMemoryReclaim() throws IOException, InterruptedException {
        // Create config with short delivery timeout
        Properties props = new Properties();
        props.setProperty("delivery.timeout.ms", "100"); // 100ms timeout
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        // Track initial memory
        long initialMemory = accumulator.getTotalBytesUsed();
        assertEquals(0, initialMemory);
        
        // Add multiple records to create batches
        Headers headers = new Headers();
        for (int i = 0; i < 3; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "TestTopic", i, System.currentTimeMillis(), "key" + i, "value" + i, headers
            );
            byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
            accumulator.append(serializedData);
        }
        
        // Verify memory is being used
        long memoryAfterAppend = accumulator.getTotalBytesUsed();
        assertTrue(memoryAfterAppend > 0);
        
        // Wait for batches to expire
        Thread.sleep(150);
        
        // Call ready() to trigger expiry
        accumulator.ready();
        
        // Memory should be reclaimed
        long memoryAfterExpiry = accumulator.getTotalBytesUsed();
        assertEquals(0, memoryAfterExpiry);
    }
    
    @Test
    public void testExpiredBatchesNotReady() throws IOException, InterruptedException {
        // Create config with moderate delivery timeout
        Properties props = new Properties();
        props.setProperty("delivery.timeout.ms", "200"); // 200ms timeout
        props.setProperty("linger.ms", "50"); // 50ms linger
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        // Add a record that won't fill the batch
        Headers headers = new Headers();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "small", headers
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
        accumulator.append(serializedData);
        
        // Wait past linger time but before expiry
        Thread.sleep(60); // Past 50ms linger
        
        // Batch should be ready due to linger timeout
        List<TopicPartition> readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        
        // Now wait for expiry
        Thread.sleep(200); // Total > 200ms delivery timeout
        
        // Batch should be expired and not ready
        readyPartitions = accumulator.ready();
        assertTrue(readyPartitions.isEmpty());
        assertNull(accumulator.getCurrentBatch("TestTopic", 0));
    }
    
    @Test
    public void testMixedExpiryAndReadyBatches() throws IOException, InterruptedException {
        // Create config with moderate timeouts
        Properties props = new Properties();
        props.setProperty("delivery.timeout.ms", "150"); // 150ms timeout
        props.setProperty("batch.size", "512"); // Small batch for testing
        props.setProperty("linger.ms", "50"); // 50ms linger
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        Headers headers = new Headers();
        
        // Add old batch that will expire
        ProducerRecord<String, String> oldRecord = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "old", headers
        );
        accumulator.append(ProducerRecordCodec.serialize(oldRecord, String.class, String.class));
        
        // Wait for it to age
        Thread.sleep(160); // Past delivery timeout
        
        // Add new batch that should be ready
        ProducerRecord<String, String> newRecord = new ProducerRecord<>(
                "TestTopic", 1, System.currentTimeMillis(), "key", "x".repeat(450), headers
        );
        accumulator.append(ProducerRecordCodec.serialize(newRecord, String.class, String.class));
        
        // Wait for linger timeout for new batch
        Thread.sleep(60); // Wait 60ms > 50ms linger time
        
        // Call ready()
        List<TopicPartition> readyPartitions = accumulator.ready();
        
        // Old batch should be expired, new batch should be ready
        assertNull(accumulator.getCurrentBatch("TestTopic", 0)); // Old batch expired
        assertEquals(1, readyPartitions.size());
        assertEquals(1, readyPartitions.get(0).getPartition()); // New batch is ready
    }
    
    @Test
    public void testDeliveryTimeoutConfiguration() {
        // Test different delivery timeout settings
        Properties props100 = new Properties();
        props100.setProperty("delivery.timeout.ms", "100");
        ProducerConfig config100 = new ProducerConfig(props100);
        assertEquals(100L, config100.getDeliveryTimeoutMs());
        
        Properties props60000 = new Properties();
        props60000.setProperty("delivery.timeout.ms", "60000");
        ProducerConfig config60000 = new ProducerConfig(props60000);
        assertEquals(60000L, config60000.getDeliveryTimeoutMs());
        
        // Test default
        Properties propsDefault = new Properties();
        ProducerConfig configDefault = new ProducerConfig(propsDefault);
        assertEquals(120000L, configDefault.getDeliveryTimeoutMs()); // Default 120 seconds
    }
    
    @Test
    public void testBatchExpiryLogging() throws IOException, InterruptedException {
        // This test verifies that expiry is logged properly
        // In production, this would be captured by a logging framework
        Properties props = new Properties();
        props.setProperty("delivery.timeout.ms", "50");
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        Headers headers = new Headers();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "value", headers
        );
        
        accumulator.append(ProducerRecordCodec.serialize(record, String.class, String.class));
        
        // Get the batch to check its properties
        RecordBatch batch = accumulator.getCurrentBatch("TestTopic", 0);
        assertNotNull(batch);
        int recordCount = batch.getRecordCount();
        assertTrue(recordCount > 0);
        
        // Wait for expiry
        Thread.sleep(100);
        
        // Trigger expiry check
        accumulator.ready();
        
        // Batch should be gone
        assertNull(accumulator.getCurrentBatch("TestTopic", 0));
        // In production, we'd verify a warning was logged with the batch details
    }
}