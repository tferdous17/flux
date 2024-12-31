package commons.header;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class PropertiesTest {
    Properties properties = new Properties();
    @Test
    public void emptyPropertiesTest() {
        System.out.println("Test 1 - Empty Properties:");
        System.out.println(properties);
    }

    @Test
    public void keyValueTest() {
        properties.setProperty("key1", "value1");
        properties.setProperty("key2", "value2");
        System.out.println(properties);
    }

    @Test
    public void containKeysTest() {
        boolean containsKey1 = properties.containsProperty("key1");
        boolean containsKey3 = properties.containsProperty("key3");
        System.out.println("\nTest 4 - Check if properties exist:");
        System.out.println("Contains key1: " + containsKey1);
        System.out.println("Contains key3: " + containsKey3);
    }

    @Test
    public void keyRemovalTest() {
        properties.removeProperty("key1");
        System.out.println("\nTest 5 - After Removing key1:");
        System.out.println(properties);
    }

    @Test
    public void allPropertiesTest() {
        properties.setProperty("key1", "value1");
        Map<String, String> allProperties = properties.getAllProperties();
        System.out.println("\nTest 6 - All Properties:");
        System.out.println(allProperties);
    }

    @Test
    public void initialPropertiesTest() {
        Map<String, String> initialProperties = new HashMap<>();
        initialProperties.put("key3", "value3");
        initialProperties.put("key4", "value4");
        Properties propertiesWithInitialData = new Properties(initialProperties);
        System.out.println("\nTest 7 - Properties with Initial Data:");
        System.out.println(propertiesWithInitialData);
    }
}
