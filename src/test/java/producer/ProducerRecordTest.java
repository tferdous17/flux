package producer;

import commons.header.Header;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProducerRecordTest {
    String key = "Bob";
    Integer value = 22;
    Header header1 = new Header("header-key-1", "value1".getBytes());
    Header header2 = new Header("header-key-2", "value2".getBytes());
    List<Header> headers = new ArrayList<>(Arrays.asList(header1,header2));

    @Test
    public void topicPartitionTimestampKeyValueTest() {
        ProducerRecord<String, Integer> test = new ProducerRecord<>("Age", 1, 1412L, key, value);
        System.out.println(test + "\n");
    }

    @Test
    public void topicPartitionTimestampKeyValueHeadersTest() {
        ProducerRecord<String, Integer> test = new ProducerRecord<>("Age", 1, 1412L, key, value, headers);
        System.out.println(test + "\n");
    }

    @Test
    public void topicPartitionKeyValueTest() {
        ProducerRecord<String, Integer> test = new ProducerRecord<>("Age", 1, key, value);
        System.out.println(test + "\n");
    }

    @Test
    public void topicPartitionKeyValueHeadersTest() {
        ProducerRecord<String, Integer> test = new ProducerRecord<>("Age", 1, key, value, headers);
        System.out.println(test + "\n");
    }

    @Test
    public void topicKeyValueTest() {
        ProducerRecord<String, Integer> test = new ProducerRecord<>("Age", key, value);
        System.out.println(test + "\n");
    }

    @Test
    public void topicValueTest() {
        ProducerRecord<String, Integer> test = new ProducerRecord<>("Age", value);
        System.out.println(test + "\n");
    }
}
