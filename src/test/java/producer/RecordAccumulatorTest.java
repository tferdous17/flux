package producer;

import commons.CompressionType;
import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import proto.Topic;
import server.internal.Broker;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class RecordAccumulatorTest {
    
    @BeforeAll
    public static void setUp() throws IOException {
        // Create broker and topics needed for tests
        Broker broker = new Broker();
        
        // Create test topics
        Topic bobTopic = Topic.newBuilder()
                .setTopicName("Bob")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
                
        Topic testTopic = Topic.newBuilder()
                .setTopicName("TestTopic")
                .setNumPartitions(5)
                .setReplicationFactor(1)
                .build();
                
        Topic topicTopic = Topic.newBuilder()
                .setTopicName("Topic")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
        
        broker.createTopics(List.of(bobTopic, testTopic, topicTopic));
    }
    
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
        RecordAccumulator recordAccumulator = new RecordAccumulator(new ProducerConfig(), 3);
        recordAccumulator.append(serializedData);
        recordAccumulator.printRecord();
    }

    @Test
    public void testMemoryTracking() throws IOException {
        ProducerConfig config = new ProducerConfig(10240, 100, 1000L, CompressionType.NONE, 60000L);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3); // Small buffer size for testing
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
        RecordAccumulator accumulator = new RecordAccumulator(new ProducerConfig(), 3);
        
        // Create records for different partitions
        Headers headers = new Headers();
        ProducerRecord<String, String> record1 = new ProducerRecord<>("Topic", 0, System.currentTimeMillis(), "key1", "value1", headers);
        ProducerRecord<String, String> record2 = new ProducerRecord<>("Topic", 1, System.currentTimeMillis(), "key2", "value2", headers);
        
        byte[] data1 = ProducerRecordCodec.serialize(record1, String.class, String.class);
        byte[] data2 = ProducerRecordCodec.serialize(record2, String.class, String.class);
        
        accumulator.append(data1);
        accumulator.append(data2);
        
        // Verify we can get batches for different topic-partitions
        assertNotNull(accumulator.getCurrentBatch("Topic", 0));
        assertNotNull(accumulator.getCurrentBatch("Topic", 1));
        assertNull(accumulator.getCurrentBatch("Topic", 2)); // No batch for partition 2 yet
    }

    @Test
    public void testGetters() {
        ProducerConfig config = new ProducerConfig(10240, 100, 32L * 1024 * 1024, CompressionType.NONE, 60000L);
        RecordAccumulator accumulator = new RecordAccumulator(config, 5);
        assertEquals(10240, accumulator.getBatchSize());
        assertEquals(32L * 1024 * 1024, accumulator.getBufferMemory());
        assertEquals(0, accumulator.getTotalBytesUsed());
    }

    @Test
    public void testDefaultConstructors() {
        RecordAccumulator accumulator1 = new RecordAccumulator(new ProducerConfig(), 3);
        assertEquals(16384, accumulator1.getBatchSize());
        assertEquals(33554432L, accumulator1.getBufferMemory());

        ProducerConfig config2 = new ProducerConfig(16384, 100, 33554432L, CompressionType.NONE, 60000L);
        RecordAccumulator accumulator2 = new RecordAccumulator(config2, 3);
        assertEquals(16384, accumulator2.getBatchSize());
        assertEquals(33554432L, accumulator2.getBufferMemory());
    }

    @Test
    public void testReadyLogic_BatchIsFull() throws IOException {
        // Create small batch size to easily fill it
        ProducerConfig config = new ProducerConfig(100, 5000, 1024L * 1024, CompressionType.GZIP, 60000L);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        // Create a record that will fill the batch
        Headers headers = new Headers();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", 
                "x".repeat(95), headers // 95 chars + overhead should fill 100 byte batch
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        // Append record to fill the batch
        accumulator.append(serializedData);
        
        // Batch should be ready because it's full
        List<TopicPartition> readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        assertEquals("TestTopic", readyPartitions.get(0).getTopic());
        assertEquals(0, readyPartitions.get(0).getPartition());
    }

    @Test 
    public void testReadyLogic_BatchTimedOut() throws IOException, InterruptedException {
        // Create config with very short linger time
        ProducerConfig config = new ProducerConfig(10240, 50, 1024L * 1024, CompressionType.GZIP, 60000L); // 50ms linger
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        // Add small record that won't fill batch
        Headers headers = new Headers();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 1, System.currentTimeMillis(), "key", "small", headers
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        accumulator.append(serializedData);
        
        // Should not be ready immediately
        List<TopicPartition> readyPartitions = accumulator.ready();
        assertEquals(0, readyPartitions.size());
        
        // Wait for linger time to pass
        Thread.sleep(60); // Wait 60ms > 50ms linger time
        
        // Should be ready due to timeout
        readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        assertEquals("TestTopic", readyPartitions.get(0).getTopic());
        assertEquals(1, readyPartitions.get(0).getPartition());
    }

    @Test
    public void testReadyLogic_MultipleBatches() throws IOException {
        ProducerConfig config = new ProducerConfig(100, 5000, 1024L * 1024, CompressionType.GZIP, 60000L);
        RecordAccumulator accumulator = new RecordAccumulator(config, 5);
        
        Headers headers = new Headers();
        
        // Fill batch for partition 0
        ProducerRecord<String, String> record1 = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "x".repeat(95), headers
        );
        accumulator.append(ProducerRecordCodec.serialize(record1, String.class, String.class));
        
        // Add small record to partition 2 (won't be full)
        ProducerRecord<String, String> record2 = new ProducerRecord<>(
                "TestTopic", 2, System.currentTimeMillis(), "key", "small", headers
        );
        accumulator.append(ProducerRecordCodec.serialize(record2, String.class, String.class));
        
        List<TopicPartition> readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        assertEquals("TestTopic", readyPartitions.get(0).getTopic());
        assertEquals(0, readyPartitions.get(0).getPartition()); // Only partition 0 should be ready (full)
    }

    @Test
    public void testDrainLogic() throws IOException {
        ProducerConfig config = new ProducerConfig(100, 5000, 1024L * 1024, CompressionType.GZIP, 60000L);
        RecordAccumulator accumulator = new RecordAccumulator(config, 5);
        
        Headers headers = new Headers();
        
        // Add records to different partitions
        ProducerRecord<String, String> record1 = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key1", "x".repeat(95), headers
        );
        ProducerRecord<String, String> record2 = new ProducerRecord<>(
                "TestTopic", 1, System.currentTimeMillis(), "key2", "x".repeat(95), headers
        );
        ProducerRecord<String, String> record3 = new ProducerRecord<>(
                "TestTopic", 2, System.currentTimeMillis(), "key3", "small", headers
        );
        
        accumulator.append(ProducerRecordCodec.serialize(record1, String.class, String.class));
        accumulator.append(ProducerRecordCodec.serialize(record2, String.class, String.class));
        accumulator.append(ProducerRecordCodec.serialize(record3, String.class, String.class));
        
        // Get ready partitions (0 and 1 should be full)
        List<TopicPartition> readyPartitions = accumulator.ready();
        assertEquals(2, readyPartitions.size());
        boolean hasPartition0 = readyPartitions.stream().anyMatch(tp -> tp.getPartition() == 0);
        boolean hasPartition1 = readyPartitions.stream().anyMatch(tp -> tp.getPartition() == 1);
        assertTrue(hasPartition0);
        assertTrue(hasPartition1);
        
        // Track initial memory usage
        long initialMemoryUsage = accumulator.getTotalBytesUsed();
        assertTrue(initialMemoryUsage > 0);
        
        // Drain ready batches
        Map<TopicPartition, RecordBatch> drainedBatches = accumulator.drain(readyPartitions);
        
        // Verify drained batches
        assertEquals(2, drainedBatches.size());
        boolean hasDrainedPartition0 = drainedBatches.keySet().stream().anyMatch(tp -> tp.getPartition() == 0);
        boolean hasDrainedPartition1 = drainedBatches.keySet().stream().anyMatch(tp -> tp.getPartition() == 1);
        boolean hasDrainedPartition2 = drainedBatches.keySet().stream().anyMatch(tp -> tp.getPartition() == 2);
        assertTrue(hasDrainedPartition0);
        assertTrue(hasDrainedPartition1);
        assertFalse(hasDrainedPartition2); // Partition 2 wasn't ready
        
        // Verify batches were removed from accumulator
        assertNull(accumulator.getCurrentBatch("TestTopic", 0));
        assertNull(accumulator.getCurrentBatch("TestTopic", 1));
        assertNotNull(accumulator.getCurrentBatch("TestTopic", 2)); // Partition 2 should still have batch
        
        // Verify memory usage decreased
        long finalMemoryUsage = accumulator.getTotalBytesUsed();
        assertTrue(finalMemoryUsage < initialMemoryUsage);
    }

    @Test
    public void testDrainWithCompression() throws IOException {
        ProducerConfig config = new ProducerConfig(1000, 5000, 1024L * 1024, CompressionType.GZIP, 60000L); // GZIP compression
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        Headers headers = new Headers();
        
        // Create record with repetitive data that should compress well
        String repetitiveData = "This is a test message that repeats. ".repeat(20);
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", repetitiveData, headers
        );
        
        accumulator.append(ProducerRecordCodec.serialize(record, String.class, String.class));
        
        // Force the batch to be ready by making it full
        List<TopicPartition> readyPartitions = List.of(new TopicPartition("TestTopic", 0));
        
        // Drain the batch
        Map<TopicPartition, RecordBatch> drainedBatches = accumulator.drain(readyPartitions);
        
        // Verify compression was applied
        RecordBatch drainedBatch = drainedBatches.get(new TopicPartition("TestTopic", 0));
        assertNotNull(drainedBatch);
        assertTrue(drainedBatch.isCompressed());
        
        // Compressed size should be smaller than original
        assertTrue(drainedBatch.getDataSize() < drainedBatch.getCurrBatchSizeInBytes());
    }

    @Test
    public void testDrainWithCompressionDisabled() throws IOException {
        ProducerConfig config = new ProducerConfig(1000, 5000, 1024L * 1024, CompressionType.NONE, 60000L); // No compression
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        Headers headers = new Headers();
        
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "test data", headers
        );
        
        accumulator.append(ProducerRecordCodec.serialize(record, String.class, String.class));
        
        List<TopicPartition> readyPartitions = List.of(new TopicPartition("TestTopic", 0));
        Map<TopicPartition, RecordBatch> drainedBatches = accumulator.drain(readyPartitions);
        
        // Verify compression was not applied
        RecordBatch drainedBatch = drainedBatches.get(new TopicPartition("TestTopic", 0));
        assertNotNull(drainedBatch);
        assertFalse(drainedBatch.isCompressed());
    }

    @Test
    public void testDrainEmptyList() throws IOException {
        RecordAccumulator accumulator = new RecordAccumulator(new ProducerConfig(), 3);
        
        // Drain with empty list should return empty map
        Map<TopicPartition, RecordBatch> drainedBatches = accumulator.drain(List.of());
        assertTrue(drainedBatches.isEmpty());
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
        props.setProperty("batch.size", "100"); // Small batch for testing
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
        
        // Add new batch that should be ready (full)
        ProducerRecord<String, String> newRecord = new ProducerRecord<>(
                "TestTopic", 1, System.currentTimeMillis(), "key", "x".repeat(95), headers
        );
        accumulator.append(ProducerRecordCodec.serialize(newRecord, String.class, String.class));
        
        // Call ready()
        List<TopicPartition> readyPartitions = accumulator.ready();
        
        // Old batch should be expired, new batch should be ready
        assertNull(accumulator.getCurrentBatch("TestTopic", 0)); // Old batch expired
        assertEquals(1, readyPartitions.size());
        assertEquals(1, readyPartitions.get(0).getPartition()); // New batch is ready
    }
}
