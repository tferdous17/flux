package producer;

import commons.FluxTopic;
import commons.header.Header;
import commons.headers.Headers;
import metadata.InMemoryTopicMetadataRepository;
import org.junit.jupiter.api.Test;
import server.internal.storage.Partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RecordAccumulatorTest {
    @Test
    public void appendTest() throws IOException {
        // Setup - Create the topic first
        String topicName = "test-topic";
        List<Partition> partitions = new ArrayList<>();
        partitions.add(new Partition("test-topic", 0)); // Add at least one partition
        FluxTopic fluxTopic = new FluxTopic(topicName, partitions, 1);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, fluxTopic);
        
        Headers headers = new Headers();
        headers.add(new Header("Kyoshi", "22".getBytes()));
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topicName,  // Use the created topic name
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
    public void testConfigurableBatchParameters() {
        int batchSize = 8192;
        int numPartitions = 3;
        long lingerMs = 200;
        long batchTimeoutMs = 15000;
        double batchSizeThreshold = 0.8;
        long bufferMemory = 16 * 1024 * 1024L; // 16MB

        RecordAccumulator accumulator = new RecordAccumulator(
            batchSize, numPartitions, lingerMs, batchTimeoutMs, batchSizeThreshold, bufferMemory);

        // Test getter methods
        assertEquals(batchSize, accumulator.getBatchSize());
        assertEquals(lingerMs, accumulator.getLingerMs());
        assertEquals(batchTimeoutMs, accumulator.getBatchTimeoutMs());
        assertEquals(batchSizeThreshold, accumulator.getBatchSizeThreshold(), 0.001);
    }

    @Test
    public void testDefaultConstructorValues() {
        RecordAccumulator accumulator = new RecordAccumulator(3);
        
        // Verify default values
        assertEquals(10240, accumulator.getBatchSize()); // 10KB default
        assertEquals(100, accumulator.getLingerMs()); // 100ms default
        assertEquals(30000, accumulator.getBatchTimeoutMs()); // 30s default
        assertEquals(0.9, accumulator.getBatchSizeThreshold(), 0.001); // 90% default
    }

    @Test
    public void testBatchSizeValidation() {
        // Test valid batch size
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 100, 30000, 0.9, 32 * 1024 * 1024L));
        
        // Test invalid batch sizes
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(0, 3, 100, 30000, 0.9, 32 * 1024 * 1024L)); // Too small
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(2 * 1024 * 1024, 3, 100, 30000, 0.9, 32 * 1024 * 1024L)); // Too large (>1MB)
    }

    @Test
    public void testLingerMsValidation() {
        // Test valid linger times
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 0, 30000, 0.9, 32 * 1024 * 1024L));
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 60000, 60000, 0.9, 32 * 1024 * 1024L));
        
        // Test invalid linger times
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, -1, 30000, 0.9, 32 * 1024 * 1024L)); // Negative
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 61000, 61000, 0.9, 32 * 1024 * 1024L)); // Too large (>1 minute)
    }

    @Test
    public void testBatchTimeoutValidation() {
        // Test valid batch timeouts
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 100, 100, 0.9, 32 * 1024 * 1024L));
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 100, 300000, 0.9, 32 * 1024 * 1024L));
        
        // Test invalid batch timeouts
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 200, 100, 0.9, 32 * 1024 * 1024L)); // Timeout < linger
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 100, 400000, 0.9, 32 * 1024 * 1024L)); // Timeout too large (>5 minutes)
    }

    @Test
    public void testBatchSizeThresholdValidation() {
        // Test valid thresholds
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 100, 30000, 0.1, 32 * 1024 * 1024L));
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 100, 30000, 1.0, 32 * 1024 * 1024L));
        
        // Test invalid thresholds
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 100, 30000, 0.0, 32 * 1024 * 1024L)); // Zero
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 100, 30000, -0.1, 32 * 1024 * 1024L)); // Negative
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 100, 30000, 1.1, 32 * 1024 * 1024L)); // > 1.0
    }

    @Test
    public void testBufferMemoryValidation() {
        // Test valid buffer memory
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 100, 30000, 0.9, 1024));
        assertDoesNotThrow(() -> new RecordAccumulator(1024, 3, 100, 30000, 0.9, 1073741824L)); // 1GB
        
        // Test invalid buffer memory
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 100, 30000, 0.9, 512)); // Less than batch size
        assertThrows(IllegalArgumentException.class, () -> 
            new RecordAccumulator(1024, 3, 100, 30000, 0.9, 1073741825L)); // > 1GB
    }

    @Test
    public void testBatchReadinessWithCustomThreshold() throws IOException, InterruptedException {
        // Setup topic
        String topicName = "threshold-test-topic";
        List<Partition> partitions = new ArrayList<>();
        partitions.add(new Partition(topicName, 0));
        FluxTopic fluxTopic = new FluxTopic(topicName, partitions, 1);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, fluxTopic);
        
        // Create accumulator with 50% threshold for testing
        RecordAccumulator accumulator = new RecordAccumulator(
            1000, 1, 5000, 10000, 0.5, 32 * 1024 * 1024L);
        
        // Create a record that will be ~250 bytes when serialized
        Headers headers = new Headers();
        headers.add(new Header("testKey", "testValue".getBytes()));
        ProducerRecord<String, String> record = new ProducerRecord<>(
            topicName, 0, System.currentTimeMillis(), "key", "a".repeat(200), headers);
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        // Add one record (should not be ready yet - less than 50% of 1000 bytes)
        accumulator.append(serializedData);
        Map<Integer, RecordBatch> readyBatches = accumulator.getReadyBatches();
        assertTrue(readyBatches.isEmpty(), "Batch should not be ready yet");
        
        // Add another record (should now exceed 50% threshold)
        accumulator.append(serializedData);
        readyBatches = accumulator.getReadyBatches();
        assertFalse(readyBatches.isEmpty(), "Batch should be ready after exceeding threshold");
    }

    @Test
    public void testBatchTimeoutForcing() throws IOException, InterruptedException {
        // Setup topic
        String topicName = "timeout-test-topic";
        List<Partition> partitions = new ArrayList<>();
        partitions.add(new Partition(topicName, 0));
        FluxTopic fluxTopic = new FluxTopic(topicName, partitions, 1);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, fluxTopic);
        
        // Create accumulator with very short timeout for testing
        RecordAccumulator accumulator = new RecordAccumulator(
            1000, 1, 5000, 100, 0.9, 32 * 1024 * 1024L); // 100ms timeout
        
        // Create a small record
        Headers headers = new Headers();
        ProducerRecord<String, String> record = new ProducerRecord<>(
            topicName, 0, System.currentTimeMillis(), "key", "small", headers);
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        // Add record
        accumulator.append(serializedData);
        
        // Should not be ready immediately
        Map<Integer, RecordBatch> readyBatches = accumulator.getReadyBatches();
        assertTrue(readyBatches.isEmpty(), "Batch should not be ready immediately");
        
        // Wait for timeout
        Thread.sleep(150);
        
        // Should be ready now due to timeout
        readyBatches = accumulator.getReadyBatches();
        assertFalse(readyBatches.isEmpty(), "Batch should be ready after timeout");
    }
}
