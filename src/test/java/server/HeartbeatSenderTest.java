package server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import server.config.BrokerConfig;
import server.internal.HeartbeatSender;

import static org.junit.jupiter.api.Assertions.*;

public class HeartbeatSenderTest {

    private HeartbeatSender heartbeatSender;
    private BrokerConfig config;

    @BeforeEach
    public void setUp() {
        config = new BrokerConfig();
        heartbeatSender = new HeartbeatSender("test-broker-1", "localhost", 9090, config);
    }

    @AfterEach
    public void tearDown() {
        if (heartbeatSender != null) {
            heartbeatSender.stop();
        }
    }

    @Test
    public void testStartAndStop() {
        // Test that heartbeat sender can be started and stopped
        assertFalse(heartbeatSender.isRunning());

        // We can test starting with an invalid endpoint - it will fail to connect
        // but the state should still be managed correctly
        heartbeatSender.start("localhost:99999"); // Invalid port

        // The sender should report as running even if connection fails
        // (it will retry in the background)
        assertTrue(heartbeatSender.isRunning());

        // Stop should work regardless
        heartbeatSender.stop();
        assertFalse(heartbeatSender.isRunning());
    }

    @Test
    public void testHeartbeatSenderCreation() {
        // Test that heartbeat sender is created with correct parameters
        assertNotNull(heartbeatSender);
        assertFalse(heartbeatSender.isRunning());
    }

    @Test
    public void testConfigurationRespected() {
        // Test that configuration is properly set
        assertNotNull(config);
        assertEquals(3, config.getMissedHeartbeatThreshold());
        assertEquals(5000, config.getHeartbeatTimeoutMs());
    }

    @Test
    public void testMultipleStopCalls() {
        // Test that multiple stop calls don't cause issues
        heartbeatSender.stop();
        heartbeatSender.stop(); // Should handle gracefully
        assertFalse(heartbeatSender.isRunning());
    }
}