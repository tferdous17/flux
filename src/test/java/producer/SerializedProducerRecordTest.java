package producer;

import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class SerializedProducerRecordTest {
    @Test
    public void serializedProducerRecordTest() {
        Headers headers = new Headers();
        headers.add(new Header("Kyoshi", "22".getBytes()));
        ProducerRecord<String, Integer> record = new ProducerRecord<>(
                "Bob",
                0,
                System.currentTimeMillis(),
                "key",
                22,
                headers
        );

        // Serialize
        byte[] serializedData = SerializedProducerRecord.serialize(record);
        System.out.println(Arrays.toString(serializedData));
        System.out.println("Serialized Data Length: " + serializedData.length + "\n");

        // Deserialize
        ProducerRecord<String, Integer> deserializedRecord = SerializedProducerRecord.deserialize(
                serializedData,
                String.class,
                Integer.class
        );
    }
}
