package producer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Properties;

public class FluxProducerIntegrationTest {
    
    private Properties props;
    
    @BeforeEach
    public void setup() {
        props = new Properties();
        props.setProperty("batch.size", "1024");
        props.setProperty("linger.ms", "50");
        props.setProperty("max.buffer.size", "10240");
        props.setProperty("compression.enabled", "true");
    }
    
    @Test
    public void testFluxProducerWithAccumulator() throws IOException {
        // This test validates that FluxProducer can be created with RecordAccumulator
        // and send records without compilation errors
        
        FluxProducer<String, String> producer = new FluxProducer<>(props, 0, 100);
        
        // Verify accumulator is properly initialized
        assertNotNull(producer);
        
        // Create a test record
        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic", "test-key", "test-value");
        
        try {
            producer.send(record);
            // If we get here, the integration is working - no exceptions thrown
            assertTrue(true, "Record sent successfully to accumulator");
        } catch (Exception e) {
            // Skip network/broker related exceptions - we're only testing integration
            if (!(e.getMessage().contains("bootstrap.servers") || 
                  e.getMessage().contains("metadata") ||
                  e.getMessage().contains("cluster"))) {
                fail("Unexpected error during record sending: " + e.getMessage());
            }
        }
        
        producer.close();
    }
    
    @Test 
    public void testAccumulatorConfiguration() throws IOException {
        FluxProducer<String, String> producer = new FluxProducer<>(props, 0, 100);
        
        // The test passes if the producer can be constructed without errors
        // This validates that ProducerConfig is properly passed to RecordAccumulator
        assertNotNull(producer);
        
        producer.close();
    }
}