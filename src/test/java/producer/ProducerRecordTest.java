package producer;

import commons.header.Header;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ProducerRecordTest {
    @Test
    public void test1() {
        String key = "Kyoshi";
        Integer value = 22;

        Header header1 = new Header("header-key-1", "value1".getBytes());
        Header header2 = new Header("header-key-2", "value2".getBytes());
        List<Header> headers = new ArrayList<>();
        headers.add(header1);
        headers.add(header2);

        ProducerRecord<String, Integer> test1 = new ProducerRecord<>("Age", 1, 1412L, key, value);
        ProducerRecord<String, Integer> test2 = new ProducerRecord<>("Age", 1, 1412L, key, value, headers);
        ProducerRecord<String, Integer> test3 = new ProducerRecord<>("Age", 1, key, value);
        ProducerRecord<String, Integer> test4 = new ProducerRecord<>("Age", 1, key, value, headers);
        ProducerRecord<String, Integer> test5 = new ProducerRecord<>("Age", key, value);
        ProducerRecord<String, Integer> test6 = new ProducerRecord<>("Age", value);


        System.out.println("TEST 1");
        System.out.println(test1 + "\n");

        System.out.println("TEST 2");
        System.out.println(test2 + "\n");

        System.out.println("TEST 3");
        System.out.println(test3 + "\n");

        System.out.println("TEST 4");
        System.out.println(test4 + "\n");

        System.out.println("TEST 5");
        System.out.println(test5 + "\n");

        System.out.println("TEST 6");
        System.out.println(test6 + "\n");
    }
}
