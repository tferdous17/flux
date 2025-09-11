package producer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FluxProducerTest {
    
    @BeforeAll
    public static void setUpServer() throws IOException {
        SharedTestServer.startServer();
        // Initialize Metadata singleton after server is ready
        // Retry a few times to ensure server is fully started
        for (int i = 0; i < 3; i++) {
            try {
                metadata.Metadata.getInstance();
                break;
            } catch (Exception e) {
                if (i == 2) throw e;
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    @Test
    public void mapConstructorTest() {
        Map<String, String> configs = new HashMap<>();
        configs.put("sample-key", "same-value");
        FluxProducer<String, String> fluxProducer = new FluxProducer<>();
        System.out.println(fluxProducer);
    }
}
