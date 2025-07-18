package producer;

import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class RecordAccumulatorTest {
    @Test
    public void appendTest() throws IOException {
        // Setup
        Headers headers = new Headers();
        headers.add(new Header("Kyoshi", "22".getBytes()));
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "Bob",
                0,
                System.currentTimeMillis(),
                "key",
                "22",
                headers
        );
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);

        // Execute
        RecordAccumulator recordAccumulator = new RecordAccumulator(3);
        recordAccumulator.append(serializedData);
        recordAccumulator.printRecord();
    }
}
