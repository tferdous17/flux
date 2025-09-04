package producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;
import static producer.ProducerTestUtils.*;

import java.io.IOException;

public class BatchCallbackIntegrationTest {
    
    private RecordAccumulator accumulator;
    private CallbackTracker callbackTracker;
    
    @BeforeEach
    void setUp() throws IOException {
        accumulator = new RecordAccumulator(1024, 3, 100);
        callbackTracker = new CallbackTracker();
        setupTestTopic("test-topic", 3);
    }
    
    @AfterEach
    void tearDown() {
        if (accumulator != null) {
            accumulator.close();
        }
    }
    
    @Test
    void testBatchLifecycle() throws IOException, InterruptedException {
        byte[] record = serializeRecord(createTestRecord("test-topic", 0, "key", "value"));
        
        accumulator.append(record);
        RecordBatch batch = accumulator.getCurrentBatch(0);
        batch.addCallback(callbackTracker);
        
        Thread.sleep(50);
        assertEquals(1, callbackTracker.getCreatedCount());
        
        accumulator.flush();
        Thread.sleep(50);
        assertEquals(1, callbackTracker.getReadyCount());
        
        accumulator.onBatchSendSuccess(batch.getBatchId(), "success");
        Thread.sleep(50);
        assertEquals(1, callbackTracker.getSuccessCount());
    }
    
    @Test
    void testBatchFailure() throws IOException, InterruptedException {
        byte[] record = serializeRecord(createTestRecord("test-topic", 0, "key", "value"));
        
        accumulator.append(record);
        RecordBatch batch = accumulator.getCurrentBatch(0);
        batch.addCallback(callbackTracker);
        
        accumulator.flush();
        RuntimeException error = new RuntimeException("Test failure");
        accumulator.onBatchSendFailure(batch.getBatchId(), error);
        
        Thread.sleep(50);
        assertEquals(1, callbackTracker.getFailureCount());
        assertEquals(error, callbackTracker.getLastException());
    }
    
    @Test
    void testDefaultCallbacks() throws IOException {
        byte[] record = serializeRecord(createTestRecord("test-topic", 0, "key", "value"));
        
        accumulator.append(record);
        RecordBatch batch = accumulator.getCurrentBatch(0);
        
        assertTrue(batch.getCallbacks().size() >= 2);
        assertTrue(batch.getCallbacks().stream().anyMatch(c -> c instanceof BufferReleaseCallback));
        assertTrue(batch.getCallbacks().stream().anyMatch(c -> c instanceof LoggingCallback));
    }
}