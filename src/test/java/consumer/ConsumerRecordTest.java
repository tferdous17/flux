package consumer;

import commons.header.Header;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;

public class ConsumerRecordTest {
    String topic = "Test Topic";
    int partition = 1;
    long offset = 1;
    long timestamp = 1704067200;
    String key = "key";
    String value = "value";
    Header header1 = new Header("Key1", "Value1".getBytes());
    Header header2 = new Header("Key2", "Value2".getBytes());
    Iterable<Header> headers = new ArrayList<>(Arrays.asList(header1, header2));
    ConsumerRecord<String, String> cr = new ConsumerRecord<>(topic, partition, offset, timestamp, key, value, headers);

    @Test
    public void constructorOneTest() {
        ConsumerRecord<String, String> test = new ConsumerRecord<>(topic, partition, offset, timestamp);
        System.out.println(test + "\n");
    }

    @Test
    public void constructorTwoTest() {
        ConsumerRecord<String, String> test = new ConsumerRecord<>(topic, partition, offset, timestamp, key, value, headers);
        System.out.println(test + "\n");
    }

    @Test
    public void testGetHeaders() {
        Iterable<Header> headers = cr.getHeaders();
        for (Header h : headers) {
            h.toString();
        }
    }

    @Test
    public void testGetKey() {
        String actualKey = cr.getKey();
        System.out.println("Key: " + actualKey);
    }

    @Test
    public void testGetValue() {
        String actualValue = cr.getValue();
        System.out.println("Value: " + actualValue);
    }

    @Test
    public void testGetTopic() {
        String actualTopic = cr.getTopic();
        System.out.println("Topic: " + actualTopic);
    }

    @Test
    public void testGetPartition() {
        int actualPartition = cr.getPartition();
        System.out.println("Partition: " + actualPartition);
    }

    @Test
    public void testGetOffset() {
        long actualOffset = cr.getOffset();
        System.out.println("Offset: " + actualOffset);
    }

    @Test
    public void testGetTimestamp() {
        long actualTimestamp = cr.getTimestamp();
        System.out.println("Timestamp: " + actualTimestamp);
    }

    @Test
    public void testToStringMethod() {
        System.out.println(cr.toString());
    }

}
