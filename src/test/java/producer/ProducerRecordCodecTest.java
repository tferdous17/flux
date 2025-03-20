package producer;

import commons.headers.Headers;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class ProducerRecordCodecTest {
    @Test
    public void serializedProducerRecordTest() {
        Headers headers = new Headers();
//        headers.add(new Header("Kyoshi", "22".getBytes()));
        ProducerRecord<String, String> record = new ProducerRecord<>(
                "test-topic",
                1,
                "key",
                "test-value"
        );

        // Serialize
        byte[] serializedData = ProducerRecordCodec.serialize(record, String.class, String.class);
        System.out.println(Arrays.toString(serializedData));
        System.out.println("Serialized Data Length: " + serializedData.length + "\n");

//         Deserialize
        ProducerRecord<String, String> deserializedRecord = ProducerRecordCodec.deserialize(
                serializedData,
                String.class,
                String.class
        );

        System.out.println("Deserialized Record: " + deserializedRecord.toString());
    }
}
