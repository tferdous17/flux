package producer;

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
        
        // Verify we can get batches for different topic-partitions
        assertNotNull(accumulator.getCurrentBatch("Topic", 0));
        assertNotNull(accumulator.getCurrentBatch("Topic", 1));
        assertNull(accumulator.getCurrentBatch("Topic", 2)); // No batch for partition 2 yet
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
        assertEquals(16384, accumulator1.getBatchSize());
        assertEquals(32 * 1024 * 1024, accumulator1.getMaxBufferSize());

        RecordAccumulator accumulator2 = new RecordAccumulator(16384, 3);
        assertEquals(16384, accumulator2.getBatchSize());
        assertEquals(32 * 1024 * 1024, accumulator2.getMaxBufferSize());
    }

    @Test
    public void testReadyLogic_BatchIsFull() throws IOException {
        // Create small batch size to easily fill it
        ProducerConfig config = new ProducerConfig(100, 5000, 1024 * 1024, true);
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
        ProducerConfig config = new ProducerConfig(10240, 50, 1024 * 1024, true); // 50ms linger
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
        ProducerConfig config = new ProducerConfig(100, 5000, 1024 * 1024, true);
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
        ProducerConfig config = new ProducerConfig(100, 5000, 1024 * 1024, true);
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
        ProducerConfig config = new ProducerConfig(1000, 5000, 1024 * 1024, true); // Compression enabled
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
        ProducerConfig config = new ProducerConfig(1000, 5000, 1024 * 1024, false); // Compression disabled
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
        RecordAccumulator accumulator = new RecordAccumulator(3);
        
        // Drain with empty list should return empty map
        Map<TopicPartition, RecordBatch> drainedBatches = accumulator.drain(List.of());
        assertTrue(drainedBatches.isEmpty());
    }
}
