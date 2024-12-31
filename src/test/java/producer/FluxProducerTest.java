package producer;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class FluxProducerTest {
    @Test
    public void mapConstructorTest() {
        Map<String, String> configs = new HashMap<>();
        configs.put("sample-key", "same-value");
        FluxProducer<String, String> fluxProducer = new FluxProducer<>(configs);
        System.out.println(fluxProducer);
    }
}
