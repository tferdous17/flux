import commons.header.Header;
import commons.header.Properties;
import producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void producerRecordTest(){
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

        System.out.println("Properties class main testing ");

        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("retries", "3");
        initialProperties.put("timeout", "5000");

        Properties producerProperties = new Properties(initialProperties);

        producerProperties.setProperty("yo mama", "max");

        System.out.println("Retries: " + producerProperties.getProperty("retries"));
        System.out.println("Timeout: " + producerProperties.getProperty("timeout"));

        // Handling the case where 'acknowledgment' does not exist
        String acknowledgment = producerProperties.getProperty("acknowledgment");
        if (acknowledgment != null) {
            System.out.println("Acknowledgment: " + acknowledgment);
        } else {
            System.out.println("Acknowledgment: Not set");
        }

        System.out.println("Contains 'retries': " + producerProperties.containsKey("retries"));

        producerProperties.removeProperty("timeout");
        System.out.println("Contains 'timeout': " + producerProperties.containsKey("timeout"));

        System.out.println("All Properties: " + producerProperties);
    }

    public static void main(String[] args) {
        producerRecordTest();
    }
}
