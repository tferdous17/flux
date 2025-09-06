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

/**
 * Core tests for RecordAccumulator functionality.
 * Tests for specific features like in-flight tracking, batch expiry,
 * and drain/ready logic have been moved to their respective test files.
 */
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
    public void testCreateBatch() throws InterruptedException {
        RecordAccumulator accumulator = new RecordAccumulator(new ProducerConfig(), 3);
        
        // Create a batch and verify it's allocated from the buffer pool
        RecordBatch batch = accumulator.createBatch(0, 100);
        assertNotNull(batch);
        assertNotNull(batch.getBuffer());
        assertEquals(accumulator.getBatchSize(), batch.getInitialCapacity());
    }
    
    @Test
    public void testGetPartitionBatches() throws IOException {
        RecordAccumulator accumulator = new RecordAccumulator(new ProducerConfig(), 3);
        
        // Initially should be empty
        Map<TopicPartition, RecordBatch> partitionBatches = accumulator.getPartitionBatches();
        assertTrue(partitionBatches.isEmpty());
        
        // Add some records
        Headers headers = new Headers();
        ProducerRecord<String, String> record1 = new ProducerRecord<>("Topic", 0, System.currentTimeMillis(), "key1", "value1", headers);
        ProducerRecord<String, String> record2 = new ProducerRecord<>("Topic", 1, System.currentTimeMillis(), "key2", "value2", headers);
        
        accumulator.append(ProducerRecordCodec.serialize(record1, String.class, String.class));
        accumulator.append(ProducerRecordCodec.serialize(record2, String.class, String.class));
        
        // Should now have 2 partition batches
        partitionBatches = accumulator.getPartitionBatches();
        assertEquals(2, partitionBatches.size());
        assertTrue(partitionBatches.containsKey(new TopicPartition("Topic", 0)));
        assertTrue(partitionBatches.containsKey(new TopicPartition("Topic", 1)));
    }
    
    @Test
    public void testBatchSizeValidation() {
        // Test batch size validation - too small
        Properties props = new Properties();
        props.setProperty("batch.size", "0");
        assertThrows(IllegalArgumentException.class, () -> {
            new ProducerConfig(props);
        });
        
        // Test batch size validation - too large (> 1MB)
        props.setProperty("batch.size", "1048577");
        assertThrows(IllegalArgumentException.class, () -> {
            new ProducerConfig(props);
        });
        
        // Valid batch size should work
        props.setProperty("batch.size", "1024");
        ProducerConfig config = new ProducerConfig(props);
        assertEquals(1024, config.getBatchSize());
    }
    
    @Test
    public void testBufferPoolAccess() {
        ProducerConfig config = new ProducerConfig();
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        BufferPool pool = accumulator.getBufferPool();
        assertNotNull(pool);
        assertEquals(config.getBufferMemory(), pool.totalMemory());
        assertEquals(config.getBufferMemory(), pool.availableMemory());
    }
    
    @Test
    public void testReenqueueMethod() {
        RecordAccumulator accumulator = new RecordAccumulator(new ProducerConfig(), 3);
        
        // Create a test batch
        RecordBatch batch = new RecordBatch(1024);
        batch.incrementRetryCount();
        
        // Create a topic partition
        TopicPartition tp = new TopicPartition("test-topic", 0);
        
        // Re-enqueue the batch
        accumulator.reenqueue(tp, batch);
        
        // Verify the batch was added
        RecordBatch retrievedBatch = accumulator.getCurrentBatch("test-topic", 0);
        assertNotNull(retrievedBatch, "Batch should be re-enqueued");
        assertEquals(1, retrievedBatch.getRetryCount(), "Retry count should be preserved");
    }
    
    @Test
    public void testProducerConfigAccess() {
        Properties props = new Properties();
        props.setProperty("batch.size", "8192");
        props.setProperty("linger.ms", "50");
        ProducerConfig config = new ProducerConfig(props);
        
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        ProducerConfig retrievedConfig = accumulator.getConfig();
        
        assertNotNull(retrievedConfig);
        assertEquals(8192, retrievedConfig.getBatchSize());
        assertEquals(50L, retrievedConfig.getLingerMs());
    }
}