package admin;

import commons.header.Properties;
import org.tinylog.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * FluxAdmin is a Singleton class to represent a single Admin client.
 * The admin client is responsible for creating topics, managing partitions per topic,
 * and much more.
 */
public class FluxAdmin implements Admin {
    private static FluxAdmin instance = null;
    private Map<String, Object> topics;
    private Properties properties;

    private FluxAdmin(Properties properties) {
        this.topics = new HashMap<>();
        this.properties = properties;
    }

    public static Admin create(Properties properties) {
        if (instance == null) {
            Logger.info("Creating new Admin client");
            instance = new FluxAdmin(properties);
        } else {
            Logger.warn("Admin client already exists");
        }
        return instance;
    }

    // TODO: Implement createTopic properly when we have support for multiple partitions and topics
    @Override
    public void createTopic(String name, int numPartitions) {
        // ! This method is just a mock. When we support topics and 2+ partitions, it will get implemented properly
        if (!this.topics.containsKey(name)) {
            topics.put(name, new Object());
        }

        Logger.info(String.format("Topic created with name %s and %d partitions", name, numPartitions));
    }

    // TODO: Properly implement increasePartitions when we have support for multiple partitions
    @Override
    public void increasePartitions(String topicName, int partitionCount) {
        if (!this.topics.containsKey(topicName)) {
            return;
        }

        // increase partitions by partitionCount
        Logger.info(String.format("Topic %s received %d additional partitions", topicName, partitionCount));
    }

    public Map<String, Object> getTopics() {
        return topics;
    }

    public Properties getProperties() {
        return properties;
    }
}
