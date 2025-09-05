package producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;

/**
 * Test the simplified RecordAccumulator without callbacks.
 */
public class RecordAccumulatorTest {
    
    private RecordAccumulator accumulator;
    
    @BeforeEach
    void setUp() {
        accumulator = new RecordAccumulator(1024, 3, 100);
    }
    
    @AfterEach
    void tearDown() {
        if (accumulator != null) {
            accumulator.close();
        }
    }
    
    @Test
    void testBatchCreationAndMetrics() throws IOException {
        // Create a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        // Initial metrics should be zero
        RecordAccumulator.AccumulatorMetrics metrics = accumulator.getMetrics();
        assertEquals(0, metrics.batchesCreated);
        assertEquals(0, metrics.recordsAppended);
        assertEquals(0, metrics.batchesSent);
        assertEquals(0, metrics.batchesFailed);
        
        // Append a record
        accumulator.append(serializedRecord);
        
        // Check metrics after append
        metrics = accumulator.getMetrics();
        assertEquals(1, metrics.batchesCreated);
        assertEquals(1, metrics.recordsAppended);
        assertEquals(0, metrics.batchesSent);
        
        // Get the batch
        RecordBatch batch = accumulator.getCurrentBatch(0);
        assertNotNull(batch);
        assertEquals(BatchState.CREATED, batch.getState());
        
        // Flush and mark as sent
        var flushed = accumulator.flush();
        assertFalse(flushed.isEmpty());
        
        String batchId = batch.getBatchId();
        accumulator.markBatchSending(batchId);
        assertEquals(BatchState.SENDING, batch.getState());
        
        accumulator.markBatchSuccess(batchId);
        
        // Check final metrics
        metrics = accumulator.getMetrics();
        assertEquals(1, metrics.batchesCreated);
        assertEquals(1, metrics.recordsAppended);
        assertEquals(1, metrics.batchesSent);
        assertEquals(0, metrics.batchesFailed);
    }
    
    @Test
    void testBatchFailureMetrics() throws IOException {
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        byte[] serializedRecord = ProducerRecordCodec.serialize(record, String.class, String.class);
        
        accumulator.append(serializedRecord);
        RecordBatch batch = accumulator.getCurrentBatch(0);
        String batchId = batch.getBatchId();
        
        var flushed = accumulator.flush();
        assertFalse(flushed.isEmpty());
        
        // Mark as failed
        accumulator.markBatchFailure(batchId, new RuntimeException("Test failure"));
        
        // Check metrics
        RecordAccumulator.AccumulatorMetrics metrics = accumulator.getMetrics();
        assertEquals(1, metrics.batchesCreated);
        assertEquals(1, metrics.recordsAppended);
        assertEquals(0, metrics.batchesSent);
        assertEquals(1, metrics.batchesFailed);
    }
    
    @Test
    void testMetricsToString() {
        RecordAccumulator.AccumulatorMetrics metrics = accumulator.getMetrics();
        String metricsStr = metrics.toString();
        
        assertTrue(metricsStr.contains("batches=(created=0, sent=0, failed=0)"));
        assertTrue(metricsStr.contains("records=0"));
        assertTrue(metricsStr.contains("inflight=0"));
    }
}