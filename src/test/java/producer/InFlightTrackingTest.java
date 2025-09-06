package producer;

import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for in-flight request tracking functionality in RecordAccumulator.
 * These tests verify that the accumulator properly tracks and limits
 * the number of concurrent requests per partition.
 */
public class InFlightTrackingTest {
    
    @BeforeAll
    public static void setUp() throws IOException {
        SharedTestServer.startServer();
    }
    
    @Test
    public void testBasicInFlightTracking() {
        Properties props = new Properties();
        props.setProperty("max.in.flight.requests", "5");
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 3);
        
        TopicPartition tp = new TopicPartition("test-topic", 0);
        
        // Initially should be 0
        assertEquals(0, accumulator.getInFlightCount(tp));
        
        // Increment in-flight
        accumulator.incrementInFlight(tp);
        assertEquals(1, accumulator.getInFlightCount(tp));
        
        // Increment multiple times
        for (int i = 1; i < 5; i++) {
            accumulator.incrementInFlight(tp);
            assertEquals(i + 1, accumulator.getInFlightCount(tp));
        }
        
        // Decrement back to zero
        for (int i = 5; i > 0; i--) {
            accumulator.decrementInFlight(tp);
            assertEquals(i - 1, accumulator.getInFlightCount(tp));
        }
    }
    
    @Test
    public void testInFlightLimitEnforcement() throws IOException, InterruptedException {
        // Create config with low max in-flight requests
        Properties props = new Properties();
        props.setProperty("max.in.flight.requests", "2");
        props.setProperty("batch.size", "512"); // Small batch to easily make them full
        props.setProperty("linger.ms", "50"); // 50ms linger
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 5);
        
        Headers headers = new Headers();
        TopicPartition tp = new TopicPartition("TestTopic", 0);
        
        // Fill a batch to make it ready
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "x".repeat(450), headers
        );
        accumulator.append(ProducerRecordCodec.serialize(record, String.class, String.class));
        
        // Wait for linger timeout
        Thread.sleep(60); // Wait 60ms > 50ms linger time
        
        // Initially, batch should be ready
        List<TopicPartition> readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        assertEquals(tp, readyPartitions.get(0));
        
        // Simulate in-flight requests at the limit
        accumulator.incrementInFlight(tp);
        accumulator.incrementInFlight(tp);
        assertEquals(2, accumulator.getInFlightCount(tp));
        
        // Create a new batch (will be ready due to timeout)
        accumulator.append(ProducerRecordCodec.serialize(record, String.class, String.class));
        Thread.sleep(60); // Wait for linger timeout
        
        // Now ready() should skip this partition due to in-flight limit
        readyPartitions = accumulator.ready();
        assertTrue(readyPartitions.isEmpty(), "Partition should not be ready when at in-flight limit");
        
        // Decrement one in-flight
        accumulator.decrementInFlight(tp);
        assertEquals(1, accumulator.getInFlightCount(tp));
        
        // Should be ready now (in-flight < max)
        readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size(), "Should be ready when in-flight count < max");
        
        // Set back to max
        accumulator.incrementInFlight(tp);
        assertEquals(2, accumulator.getInFlightCount(tp));
        
        // Should not be ready at max
        readyPartitions = accumulator.ready();
        assertTrue(readyPartitions.isEmpty());
        
        // Decrement to zero in-flight
        accumulator.decrementInFlight(tp);
        accumulator.decrementInFlight(tp);
        assertEquals(0, accumulator.getInFlightCount(tp));
        
        // Now should be ready again
        readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        assertEquals(tp, readyPartitions.get(0));
    }
    
    @Test
    public void testInFlightLimitPerPartition() throws IOException, InterruptedException {
        // Create config with max in-flight = 1
        Properties props = new Properties();
        props.setProperty("max.in.flight.requests", "1");
        props.setProperty("batch.size", "512");
        props.setProperty("linger.ms", "50"); // 50ms linger
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 5);
        
        Headers headers = new Headers();
        TopicPartition tp0 = new TopicPartition("TestTopic", 0);
        TopicPartition tp1 = new TopicPartition("TestTopic", 1);
        
        // Fill batches for two partitions
        ProducerRecord<String, String> record0 = new ProducerRecord<>(
                "TestTopic", 0, System.currentTimeMillis(), "key", "x".repeat(450), headers
        );
        ProducerRecord<String, String> record1 = new ProducerRecord<>(
                "TestTopic", 1, System.currentTimeMillis(), "key", "x".repeat(450), headers
        );
        
        accumulator.append(ProducerRecordCodec.serialize(record0, String.class, String.class));
        accumulator.append(ProducerRecordCodec.serialize(record1, String.class, String.class));
        
        // Wait for linger timeout
        Thread.sleep(60); // Wait 60ms > 50ms linger time
        
        // Both should be ready initially
        List<TopicPartition> readyPartitions = accumulator.ready();
        assertEquals(2, readyPartitions.size());
        
        // Set partition 0 to have max in-flight
        accumulator.incrementInFlight(tp0);
        
        // Only partition 1 should be ready now
        readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        assertEquals(tp1, readyPartitions.get(0));
        
        // Set partition 1 to also have max in-flight
        accumulator.incrementInFlight(tp1);
        
        // No partitions should be ready
        readyPartitions = accumulator.ready();
        assertTrue(readyPartitions.isEmpty());
        
        // Free up partition 0
        accumulator.decrementInFlight(tp0);
        
        // Add another batch to partition 0
        accumulator.append(ProducerRecordCodec.serialize(record0, String.class, String.class));
        Thread.sleep(60); // Wait for linger timeout
        
        // Partition 0 should be ready, partition 1 still blocked
        readyPartitions = accumulator.ready();
        assertEquals(1, readyPartitions.size());
        assertEquals(tp0, readyPartitions.get(0));
    }
    
    @Test
    public void testMultiplePartitionsIndependentTracking() {
        Properties props = new Properties();
        props.setProperty("max.in.flight.requests", "3");
        ProducerConfig config = new ProducerConfig(props);
        RecordAccumulator accumulator = new RecordAccumulator(config, 5);
        
        TopicPartition tp0 = new TopicPartition("topic", 0);
        TopicPartition tp1 = new TopicPartition("topic", 1);
        TopicPartition tp2 = new TopicPartition("topic", 2);
        
        // Each partition should track independently
        accumulator.incrementInFlight(tp0);
        accumulator.incrementInFlight(tp0);
        
        accumulator.incrementInFlight(tp1);
        
        // tp0 has 2, tp1 has 1, tp2 has 0
        assertEquals(2, accumulator.getInFlightCount(tp0));
        assertEquals(1, accumulator.getInFlightCount(tp1));
        assertEquals(0, accumulator.getInFlightCount(tp2));
        
        // Decrement tp0 shouldn't affect others
        accumulator.decrementInFlight(tp0);
        assertEquals(1, accumulator.getInFlightCount(tp0));
        assertEquals(1, accumulator.getInFlightCount(tp1));
        assertEquals(0, accumulator.getInFlightCount(tp2));
    }
    
    @Test
    public void testInFlightWithDifferentMaxSettings() {
        // Test with different max.in.flight.requests settings
        Properties props1 = new Properties();
        props1.setProperty("max.in.flight.requests", "1");
        ProducerConfig config1 = new ProducerConfig(props1);
        assertEquals(1, config1.getMaxInFlightRequests());
        
        Properties props5 = new Properties();
        props5.setProperty("max.in.flight.requests", "5");
        ProducerConfig config5 = new ProducerConfig(props5);
        assertEquals(5, config5.getMaxInFlightRequests());
        
        Properties propsDefault = new Properties();
        ProducerConfig configDefault = new ProducerConfig(propsDefault);
        assertEquals(5, configDefault.getMaxInFlightRequests()); // Default should be 5
    }
}