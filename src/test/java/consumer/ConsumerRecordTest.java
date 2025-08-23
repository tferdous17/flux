package consumer;

import commons.header.Header;
import commons.headers.Headers;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

public class ConsumerRecordTest {
    String topic = "Test Topic";
    int partition = 0;
    long offset = 1;
    long timestamp = 1704067200;
    String key = "key";
    String value = "value";
    Header header1 = new Header("Key1", "Value1".getBytes());
    Header header2 = new Header("Key2", "Value2".getBytes());
    Headers headers = new Headers();
    ConsumerRecord<String, String> cr = new ConsumerRecord<>(topic, partition, offset, timestamp, key, value, headers);

    @Test
    public void testGetHeaders() {
        headers.add(header1);
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

}
