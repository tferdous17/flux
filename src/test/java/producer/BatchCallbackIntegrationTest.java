package producer;

import commons.FluxTopic;
import metadata.InMemoryTopicMetadataRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import server.internal.storage.Partition;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Integration test for the batch callback system with RecordAccumulator.
 * Tests the complete flow from batch creation to completion callbacks.
 */
public class BatchCallbackIntegrationTest {
    
    private RecordAccumulator accumulator;
    private CallbackTracker callbackTracker;
    
    @BeforeEach
    void setUp() {
        accumulator = new RecordAccumulator(1024, 3, 100);
        callbackTracker = new CallbackTracker();
        
        // Setup topic metadata for tests
        setupTestTopic("test-topic", 3);
    }
    
    /**
     * Helper method to set up topic metadata for testing
     */
    private void setupTestTopic(String topicName, int numPartitions) {
        List<Partition> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            try {
                partitions.add(new Partition(topicName, i));
            } catch (IOException e) {
                throw new RuntimeException("Failed to create partition for testing", e);
            }
        }
        FluxTopic fluxTopic = new FluxTopic(topicName, partitions, 1);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(topicName, fluxTopic);
    }
    
    @AfterEach
    void tearDown() {
        if (accumulator != null) {
            accumulator.close();
        }
    }
    
    @Test
    void testBatchLifecycleWithCallbacks() throws IOException, InterruptedException {
        // Create a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        // Append record to accumulator - this should create a batch and trigger created callback
        accumulator.append(serializedRecord);
        
        // Get the created batch to register additional callbacks
        RecordBatch batch = accumulator.getCurrentBatch(0);
        assertNotNull(batch);
        
        String batchId = batch.getBatchId();
        batch.addCallback(callbackTracker);
        
        Thread.sleep(100); // Allow async callback execution
        assertEquals(1, callbackTracker.getCreatedCount());
        
        // Force batch to be ready by calling flush
        var readyBatches = accumulator.flush();
        assertFalse(readyBatches.isEmpty());
        
        Thread.sleep(100); // Allow async callback execution
        assertEquals(1, callbackTracker.getReadyCount());
        
        // Simulate successful send
        accumulator.onBatchSendSuccess(batchId, "test-ack-result");
        
        Thread.sleep(100); // Allow async callback execution
        assertEquals(1, callbackTracker.getSuccessCount());
        assertEquals("test-ack-result", callbackTracker.getLastResult());
    }
    
    @Test
    void testBatchFailureCallback() throws IOException, InterruptedException {
        // Create and append a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        accumulator.append(serializedRecord);
        
        RecordBatch batch = accumulator.getCurrentBatch(0);
        String batchId = batch.getBatchId();
        batch.addCallback(callbackTracker);
        
        // Force batch to be ready
        var readyBatches = accumulator.flush();
        assertFalse(readyBatches.isEmpty());
        
        Thread.sleep(100);
        assertEquals(1, callbackTracker.getReadyCount());
        
        // Simulate failure
        RuntimeException testException = new RuntimeException("Test failure");
        accumulator.onBatchSendFailure(batchId, testException);
        
        Thread.sleep(100);
        assertEquals(1, callbackTracker.getFailureCount());
        assertEquals(testException, callbackTracker.getLastException());
    }
    
    @Test
    void testMultipleBatchesWithCallbacks() throws IOException, InterruptedException {
        // Create multiple batches by adding records to different partitions
        for (int i = 0; i < 3; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", i, "key-" + i, "value-" + i);
            byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);
            accumulator.append(serializedRecord);
            
            // Add callback tracker to each batch
            RecordBatch batch = accumulator.getCurrentBatch(i);
            if (batch != null) {
                batch.addCallback(callbackTracker);
            }
        }
        
        Thread.sleep(100);
        // Should have created 3 batches
        assertEquals(3, callbackTracker.getCreatedCount());
        
        // Flush all batches
        var readyBatches = accumulator.flush();
        assertEquals(3, readyBatches.size());
        
        Thread.sleep(100);
        assertEquals(3, callbackTracker.getReadyCount());
        
        // Simulate success for all batches
        for (RecordBatch batch : readyBatches.values()) {
            accumulator.onBatchSendSuccess(batch.getBatchId(), "success-" + batch.getBatchId());
        }
        
        Thread.sleep(100);
        assertEquals(3, callbackTracker.getSuccessCount());
    }
    
    @Test
    void testCallbackRegistryStats() throws IOException {
        // Create a batch and add callback
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        accumulator.append(serializedRecord);
        RecordBatch batch = accumulator.getCurrentBatch(0);
        batch.addCallback(callbackTracker);
        
        // Check stats
        String stats = accumulator.getCallbackStats();
        assertNotNull(stats);
        assertTrue(stats.contains("Batches:"));
        assertTrue(stats.contains("Total Callbacks:"));
        assertTrue(stats.contains("Active:"));
    }
    
    @Test
    void testDefaultCallbacksAreAddedAutomatically() throws IOException, InterruptedException {
        // Create a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        // Append record - this should automatically add default callbacks
        accumulator.append(serializedRecord);
        
        RecordBatch batch = accumulator.getCurrentBatch(0);
        assertNotNull(batch);
        
        // Should have at least 2 default callbacks (BufferReleaseCallback, LoggingCallback)
        assertTrue(batch.getCallbacks().size() >= 2);
        
        // Verify callback types
        boolean hasBufferCallback = false;
        boolean hasLoggingCallback = false;
        
        for (BatchCallback callback : batch.getCallbacks()) {
            if (callback instanceof BufferReleaseCallback) {
                hasBufferCallback = true;
            } else if (callback instanceof LoggingCallback) {
                hasLoggingCallback = true;
            }
        }
        
        assertTrue(hasBufferCallback, "Should have BufferReleaseCallback");
        assertTrue(hasLoggingCallback, "Should have LoggingCallback");
    }
    
    /**
     * Helper class to track callback invocations for testing.
     */
    private static class CallbackTracker implements BatchCallback {
        private final AtomicInteger createdCount = new AtomicInteger(0);
        private final AtomicInteger readyCount = new AtomicInteger(0);
        private final AtomicInteger sendingCount = new AtomicInteger(0);
        private final AtomicInteger successCount = new AtomicInteger(0);
        private final AtomicInteger failureCount = new AtomicInteger(0);
        private final AtomicReference<Object> lastResult = new AtomicReference<>();
        private final AtomicReference<Throwable> lastException = new AtomicReference<>();
        
        @Override
        public void onBatchCreated(BatchInfo batchInfo) {
            createdCount.incrementAndGet();
        }
        
        @Override
        public void onBatchReady(BatchInfo batchInfo) {
            readyCount.incrementAndGet();
        }
        
        @Override
        public void onBatchSending(BatchInfo batchInfo) {
            sendingCount.incrementAndGet();
        }
        
        @Override
        public void onBatchSuccess(BatchInfo batchInfo, Object result) {
            successCount.incrementAndGet();
            lastResult.set(result);
        }
        
        @Override
        public void onBatchFailure(BatchInfo batchInfo, Throwable exception) {
            failureCount.incrementAndGet();
            lastException.set(exception);
        }
        
        public int getCreatedCount() { return createdCount.get(); }
        public int getReadyCount() { return readyCount.get(); }
        public int getSendingCount() { return sendingCount.get(); }
        public int getSuccessCount() { return successCount.get(); }
        public int getFailureCount() { return failureCount.get(); }
        public Object getLastResult() { return lastResult.get(); }
        public Throwable getLastException() { return lastException.get(); }
    }
}