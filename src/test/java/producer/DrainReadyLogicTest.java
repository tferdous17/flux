package producer;

import commons.CompressionType;
import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import proto.Topic;
import server.internal.Broker;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for drain and ready logic in RecordAccumulator.
 * These tests verify the batching, readiness detection, and draining
 * mechanisms that determine when batches are sent to brokers.
 */
public class DrainReadyLogicTest {
    
    @BeforeAll
    public static void setUp() throws IOException {
        // Create broker and register TestTopic for tests that need it
        Broker broker = new Broker();
        
        Topic testTopic = Topic.newBuilder()
                .setTopicName("TestTopic")
                .setNumPartitions(5)
                .setReplicationFactor(1)
                .build();
        
        broker.createTopics(List.of(testTopic));
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
    public void testLingerMsConfiguration() {
        // Test different linger.ms settings
        Properties props10 = new Properties();
        props10.setProperty("linger.ms", "10");
        ProducerConfig config10 = new ProducerConfig(props10);
        assertEquals(10L, config10.getLingerMs());
        
        Properties props1000 = new Properties();
        props1000.setProperty("linger.ms", "1000");
        ProducerConfig config1000 = new ProducerConfig(props1000);
        assertEquals(1000L, config1000.getLingerMs());
        
        // Test default
        Properties propsDefault = new Properties();
        ProducerConfig configDefault = new ProducerConfig(propsDefault);
        assertEquals(100L, configDefault.getLingerMs()); // Default 100ms
    }
}