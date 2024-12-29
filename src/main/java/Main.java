import commons.header.Header;
import commons.header.Properties;
import producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void producerRecordTest(){
//        String key = "Kyoshi";
//        Integer value = 22;
//
//        Header header1 = new Header("header-key-1", "value1".getBytes());
//        Header header2 = new Header("header-key-2", "value2".getBytes());
//        List<Header> headers = new ArrayList<>();
//        headers.add(header1);
//        headers.add(header2);
//
//        ProducerRecord<String, Integer> test1 = new ProducerRecord<>("Age", 1, 1412L, key, value);
//        ProducerRecord<String, Integer> test2 = new ProducerRecord<>("Age", 1, 1412L, key, value, headers);
//        ProducerRecord<String, Integer> test3 = new ProducerRecord<>("Age", 1, key, value);
//        ProducerRecord<String, Integer> test4 = new ProducerRecord<>("Age", 1, key, value, headers);
//        ProducerRecord<String, Integer> test5 = new ProducerRecord<>("Age", key, value);
//        ProducerRecord<String, Integer> test6 = new ProducerRecord<>("Age", value);
//
//
//        System.out.println("TEST 1");
//        System.out.println(test1 + "\n");
//
//        System.out.println("TEST 2");
//        System.out.println(test2 + "\n");
//
//        System.out.println("TEST 3");
//        System.out.println(test3 + "\n");
//
//        System.out.println("TEST 4");
//        System.out.println(test4 + "\n");
//
//        System.out.println("TEST 5");
//        System.out.println(test5 + "\n");
//
//        System.out.println("TEST 6");
//        System.out.println(test6 + "\n");

        Properties properties = new Properties();
        System.out.println("Test 1 - Empty Properties:");
        System.out.println(properties);

        properties.setProperty("key1", "value1");
        properties.setProperty("key2", "value2");
        System.out.println("\nTest 2 - After Setting Properties:");
        System.out.println(properties);

        String value1 = properties.getProperty("key1");
        String value2 = properties.getProperty("key2");
        System.out.println("\nTest 3 - Getting Properties:");
        System.out.println("key1: " + value1);
        System.out.println("key2: " + value2);

        boolean containsKey1 = properties.containsProperty("key1");
        boolean containsKey3 = properties.containsProperty("key3");
        System.out.println("\nTest 4 - Check if properties exist:");
        System.out.println("Contains key1: " + containsKey1);
        System.out.println("Contains key3: " + containsKey3);

        properties.removeProperty("key1");
        System.out.println("\nTest 5 - After Removing key1:");
        System.out.println(properties);

        Map<String, String> allProperties = properties.getAllProperties();
        System.out.println("\nTest 6 - All Properties:");
        System.out.println(allProperties);

        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key3", "value3");
        initialProperties.put("key4", "value4");
        Properties propertiesWithInitialData = new Properties(initialProperties);
        System.out.println("\nTest 7 - Properties with Initial Data:");
        System.out.println(propertiesWithInitialData);
    }

    public static void main(String[] args) {
        producerRecordTest();
    }
}
