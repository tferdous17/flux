package producer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for FluxProducer configuration validation.
 * Tests focus on configuration validation logic through RecordAccumulator.
 */
public class FluxProducerTest {
    
    @Test
    public void testConfigurationValidation() {
        // Test invalid batch size
        assertThrows(IllegalArgumentException.class, () -> {
            new RecordAccumulator(0, 3, 100, 30000, 0.9, 32 * 1024 * 1024L);
        }, "Should throw exception for batch size <= 0");
        
        // Test invalid batch size threshold
        assertThrows(IllegalArgumentException.class, () -> {
            new RecordAccumulator(1024, 3, 100, 30000, 1.5, 32 * 1024 * 1024L);
        }, "Should throw exception for batch size threshold > 1");
        
        assertThrows(IllegalArgumentException.class, () -> {
            new RecordAccumulator(1024, 3, 100, 30000, -0.1, 32 * 1024 * 1024L);
        }, "Should throw exception for batch size threshold < 0");
    }
    
    @Test
    public void testValidConfigurationValues() {
        // Test valid configuration doesn't throw
        assertDoesNotThrow(() -> {
            RecordAccumulator accumulator = new RecordAccumulator(8192, 3, 200, 15000, 0.8, 16 * 1024 * 1024L);
            assertEquals(8192, accumulator.getBatchSize());
            assertEquals(200, accumulator.getLingerMs());
            assertEquals(15000, accumulator.getBatchTimeoutMs());
            assertEquals(0.8, accumulator.getBatchSizeThreshold(), 0.001);
            accumulator.close();
        });
    }
}