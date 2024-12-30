package commons.header;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PropertiesTest {
    @Test
    public void test1() {
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
}
