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

}