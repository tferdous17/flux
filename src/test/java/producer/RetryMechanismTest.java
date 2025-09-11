package producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import java.util.Properties;
import java.util.ArrayDeque;
import java.util.Deque;

import static org.junit.jupiter.api.Assertions.*;

public class RetryMechanismTest {
    
    private RecordAccumulator accumulator;
    private ProducerConfig config;
    
    @BeforeEach
    public void setup() {
        Properties props = new Properties();
        props.setProperty("retries", "3");
        props.setProperty("max.in.flight.requests", "5");
        config = new ProducerConfig(props);
        accumulator = new RecordAccumulator(config, 4);
    }
    
    @Test
    public void testReenqueueMethod() {
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
    public void testInFlightTracking() {
        TopicPartition tp = new TopicPartition("test-topic", 0);
        
        // Initially should be 0
        assertEquals(0, accumulator.getInFlightCount(tp));
        
        // Increment in-flight
        accumulator.incrementInFlight(tp);
        assertEquals(1, accumulator.getInFlightCount(tp));
        
        // Increment again
        accumulator.incrementInFlight(tp);
        assertEquals(2, accumulator.getInFlightCount(tp));
        
        // Decrement
        accumulator.decrementInFlight(tp);
        assertEquals(1, accumulator.getInFlightCount(tp));
        
        // Decrement again
        accumulator.decrementInFlight(tp);
        assertEquals(0, accumulator.getInFlightCount(tp));
    }
    
    @Test
    public void testRetryCountIncrement() {
        RecordBatch batch = new RecordBatch(1024);
        
        // Initial retry count should be 0
        assertEquals(0, batch.getRetryCount());
        
        // Increment retry count
        batch.incrementRetryCount();
        assertEquals(1, batch.getRetryCount());
        
        // Increment multiple times
        batch.incrementRetryCount();
        batch.incrementRetryCount();
        assertEquals(3, batch.getRetryCount());
    }
    
    @Test
    public void testMaxRetries() {
        RecordBatch batch = new RecordBatch(1024);
        
        // Simulate retries up to the configured limit
        for (int i = 0; i < config.getRetries(); i++) {
            assertTrue(batch.getRetryCount() < config.getRetries(), 
                      "Should be able to retry when count < max retries");
            batch.incrementRetryCount();
        }
        
        // Now we've hit the max
        assertEquals(config.getRetries(), batch.getRetryCount());
        assertFalse(batch.getRetryCount() < config.getRetries(), 
                   "Should not retry when count >= max retries");
    }
}