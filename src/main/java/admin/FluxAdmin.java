package admin;

import commons.header.Properties;
import org.tinylog.Logger;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FluxAdmin is a Singleton class to represent a single Admin client.
 * The admin client is responsible for creating topics, managing partitions per topic,
 * and much more.
 */
public class FluxAdmin implements Admin {
    private static FluxAdmin instance = null;
    private List<String> bootstrapServerAddrs; // addresses of initial brokers (localhost:8080..etc)


    private FluxAdmin(List<String> bootstrapServerAddrs) {
        this.bootstrapServerAddrs = bootstrapServerAddrs;
    }

    public static Admin create(List<String> bootstrapServerAddrs) {
        if (instance == null) {
            Logger.info("Creating new Admin client");
            instance = new FluxAdmin(bootstrapServerAddrs);
        } else {
            Logger.warn("Admin client already exists");
        }
        return instance;
    }

    @Override
    public void createTopics(Collection<NewTopic> topics) {

    }

}
