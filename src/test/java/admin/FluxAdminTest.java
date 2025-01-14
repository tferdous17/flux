package admin;

import commons.header.Properties;
import org.junit.jupiter.api.Test;

public class FluxAdminTest {

    @Test
    public void createAdminClientTest() {
        Admin admin = FluxAdmin.create(new Properties());
    }

    @Test
    public void createMultipleAdminClientsTest() {
        Admin admin = FluxAdmin.create(new Properties());
        // should give warning in console b/c admin
        Admin admin2 = FluxAdmin.create(new Properties());
    }

    @Test
    public void createTopicTest() {
        Admin admin = FluxAdmin.create(new Properties());
        admin.createTopic("topic1", 1);
    }

    @Test
    public void increasePartitionsTest() {
        Admin admin = FluxAdmin.create(new Properties());
        admin.createTopic("topic1", 1);
        admin.increasePartitions("topic1", 3);
    }
}
