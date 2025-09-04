package producer;

import org.junit.jupiter.api.Test;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

public class FluxProducerTest {
    
    @Test
    public void testDefaultProducer() {
        FluxProducer<String, String> producer = new FluxProducer<>();
        assertNotNull(producer);
    }

    @Test
    public void testValidConfiguration() {
        Properties props = new Properties();
        props.setProperty("batch.size", "8192");
        props.setProperty("linger.ms", "200");
        
        assertDoesNotThrow(() -> {
            FluxProducer<String, String> producer = new FluxProducer<>(props, 60, 60);
            assertEquals(8192, producer.getAccumulator().getBatchSize());
            assertEquals(200, producer.getAccumulator().getLingerMs());
            producer.close();
        });
    }
    
    @Test
    public void testInvalidConfiguration() {
        Properties props = new Properties();
        props.setProperty("batch.size", "0");
        props.setProperty("batch.size.threshold", "1.5");
        
        assertThrows(IllegalArgumentException.class, () -> 
            new FluxProducer<String, String>(props, 60, 60));
    }
}
