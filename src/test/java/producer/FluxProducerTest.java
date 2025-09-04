package producer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class FluxProducerTest {
    @Test
    public void mapConstructorTest() {
        Map<String, String> configs = new HashMap<>();
        configs.put("sample-key", "same-value");
        FluxProducer<String, String> fluxProducer = new FluxProducer<>();
        System.out.println(fluxProducer);
    }

    @Test
    public void testProducerWithCustomBatchConfiguration() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("batch.size", "8192");
        props.setProperty("linger.ms", "200");
        props.setProperty("batch.timeout.ms", "15000");
        props.setProperty("batch.size.threshold", "0.8");
        props.setProperty("buffer.memory", "16777216"); // 16MB
        props.setProperty("retries", "5");
        props.setProperty("retry.backoff.ms", "2000");

        // This should not throw any exceptions
        assertDoesNotThrow(() -> {
            FluxProducer<String, String> producer = new FluxProducer<>(props, 60, 60);
            
            // Verify the RecordAccumulator was initialized with correct parameters
            RecordAccumulator accumulator = producer.getAccumulator();
            assertEquals(8192, accumulator.getBatchSize());
            assertEquals(200, accumulator.getLingerMs());
            assertEquals(15000, accumulator.getBatchTimeoutMs());
            assertEquals(0.8, accumulator.getBatchSizeThreshold(), 0.001);
            
            producer.close();
        });
    }

    @Test
    public void testProducerWithDefaultConfiguration() throws InterruptedException {
        Properties props = new Properties();
        
        // Test with default values
        assertDoesNotThrow(() -> {
            FluxProducer<String, String> producer = new FluxProducer<>(props, 60, 60);
            
            // Verify default values were used
            RecordAccumulator accumulator = producer.getAccumulator();
            assertEquals(10240, accumulator.getBatchSize()); // 10KB default
            assertEquals(100, accumulator.getLingerMs()); // 100ms default
            assertEquals(30000, accumulator.getBatchTimeoutMs()); // 30s default
            assertEquals(0.9, accumulator.getBatchSizeThreshold(), 0.001); // 90% default
            
            producer.close();
        });
    }

    @Test
    public void testProducerWithInvalidConfiguration() {
        Properties props = new Properties();
        props.setProperty("batch.size", "0"); // Invalid: too small
        props.setProperty("linger.ms", "100");
        props.setProperty("batch.timeout.ms", "50"); // Invalid: less than linger
        props.setProperty("batch.size.threshold", "1.5"); // Invalid: > 1.0
        
        // This should throw IllegalArgumentException due to invalid configuration
        assertThrows(IllegalArgumentException.class, () -> {
            new FluxProducer<String, String>(props, 60, 60);
        });
    }

    @Test
    public void testProducerConfigurationParsing() throws InterruptedException {
        Properties props = new Properties();
        
        // Test boundary values
        props.setProperty("batch.size", "1"); // Minimum valid size
        props.setProperty("linger.ms", "0"); // Minimum valid linger
        props.setProperty("batch.timeout.ms", "0"); // Equal to linger (valid)
        props.setProperty("batch.size.threshold", "1.0"); // Maximum valid threshold
        props.setProperty("buffer.memory", "1"); // Minimum valid (equal to batch size)
        
        assertDoesNotThrow(() -> {
            FluxProducer<String, String> producer = new FluxProducer<>(props, 60, 60);
            
            RecordAccumulator accumulator = producer.getAccumulator();
            assertEquals(1, accumulator.getBatchSize());
            assertEquals(0, accumulator.getLingerMs());
            assertEquals(0, accumulator.getBatchTimeoutMs());
            assertEquals(1.0, accumulator.getBatchSizeThreshold(), 0.001);
            
            producer.close();
        });
    }
}
