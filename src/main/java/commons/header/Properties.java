package commons.header;

import java.util.HashMap;
import java.util.Map;

public class Properties {
    private Map<String, String> properties;

    // Default constructor that initializes an empty properties map
    public Properties() {
        this.properties = new HashMap<>();
    }
    // Constructor that initializes the properties with a deep copy of the provided map
    public Properties(Map<String, String> properties) {
        this.properties = new HashMap<>(properties);
    }

    public void setProperty(String key, String value) {
        properties.put(key, value);
    }

    public String getProperty(String key) {
        return properties.get(key);

    }
    public void removeProperty(String key) {
        properties.remove(key);
    }

    public boolean containsProperty(String key) {
        return properties.containsKey(key);
    }
    // Method to get all properties and returns without exposing the internal map
    public Map<String, String> getAllProperties() {
        return this.properties;
    }

    @Override
    public String toString() {
        return "Properties{" +
                "properties=" + properties +
                '}';
    }

}