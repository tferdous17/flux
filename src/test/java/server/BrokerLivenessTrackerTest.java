package server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import server.config.BrokerConfig;
import server.internal.BrokerLivenessTracker;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class BrokerLivenessTrackerTest {

    private BrokerLivenessTracker tracker;
    private BrokerConfig config;

    @BeforeEach
    public void setUp() {
        config = new BrokerConfig();
        tracker = new BrokerLivenessTracker(config);
    }

    @AfterEach
    public void tearDown() {
        if (tracker != null) {
            tracker.stopMonitoring();
        }
    }

    @Test
    public void testRecordHeartbeat() {
        // Test recording heartbeats from multiple brokers
        long timestamp = System.currentTimeMillis();

        tracker.recordHeartbeat("broker-1", timestamp, 1);
        tracker.recordHeartbeat("broker-2", timestamp, 1);
        tracker.recordHeartbeat("broker-3", timestamp, 1);

        // Verify all brokers are tracked as alive
        assertTrue(tracker.isBrokerAlive("broker-1"));
        assertTrue(tracker.isBrokerAlive("broker-2"));
        assertTrue(tracker.isBrokerAlive("broker-3"));
        assertEquals(3, tracker.getAliveBrokerCount());
    }

    @Test
    public void testGetAliveBrokers() {
        // Test getting list of alive brokers
        long timestamp = System.currentTimeMillis();

        tracker.recordHeartbeat("broker-1", timestamp, 1);
        tracker.recordHeartbeat("broker-2", timestamp, 1);

        Set<String> aliveBrokers = tracker.getAliveBrokers();
        assertEquals(2, aliveBrokers.size());
        assertTrue(aliveBrokers.contains("broker-1"));
        assertTrue(aliveBrokers.contains("broker-2"));
    }

    @Test
    public void testMarkBrokerOffline() {
        // Test manually marking a broker as offline
        long timestamp = System.currentTimeMillis();

        tracker.recordHeartbeat("broker-1", timestamp, 1);
        assertTrue(tracker.isBrokerAlive("broker-1"));

        tracker.markBrokerOffline("broker-1");
        assertFalse(tracker.isBrokerAlive("broker-1"));
        assertEquals(0, tracker.getAliveBrokerCount());
    }

    @Test
    public void testGetLastHeartbeatTimestamp() {
        // Test getting last heartbeat timestamp
        long timestamp = System.currentTimeMillis();

        tracker.recordHeartbeat("broker-1", timestamp, 1);
        assertEquals(timestamp, tracker.getLastHeartbeatTimestamp("broker-1"));

        // Non-existent broker should return -1
        assertEquals(-1, tracker.getLastHeartbeatTimestamp("non-existent"));
    }

    @Test
    public void testRemoveBroker() {
        // Test removing a broker from tracking
        long timestamp = System.currentTimeMillis();

        tracker.recordHeartbeat("broker-1", timestamp, 1);
        tracker.recordHeartbeat("broker-2", timestamp, 1);
        assertEquals(2, tracker.getAliveBrokerCount());

        tracker.removeBroker("broker-1");
        assertFalse(tracker.isBrokerAlive("broker-1"));
        assertTrue(tracker.isBrokerAlive("broker-2"));
        assertEquals(1, tracker.getAliveBrokerCount());
    }

    @Test
    public void testHeartbeatUpdates() {
        // Test that subsequent heartbeats update the timestamp
        long timestamp1 = System.currentTimeMillis();
        long timestamp2 = timestamp1 + 1000;

        tracker.recordHeartbeat("broker-1", timestamp1, 1);
        assertEquals(timestamp1, tracker.getLastHeartbeatTimestamp("broker-1"));

        tracker.recordHeartbeat("broker-1", timestamp2, 2);
        assertEquals(timestamp2, tracker.getLastHeartbeatTimestamp("broker-1"));
    }

    @Test
    public void testStartStopMonitoring() {
        // Test starting and stopping monitoring
        tracker.startMonitoring();
        // Monitoring is now running in background

        tracker.stopMonitoring();
        // Monitoring should be stopped

        // Multiple stop calls should be safe
        tracker.stopMonitoring();
    }

    @Test
    public void testBrokerRevival() {
        // Test that an offline broker can come back online
        long timestamp = System.currentTimeMillis();

        tracker.recordHeartbeat("broker-1", timestamp, 1);
        tracker.markBrokerOffline("broker-1");
        assertFalse(tracker.isBrokerAlive("broker-1"));

        // Send new heartbeat to revive broker
        tracker.recordHeartbeat("broker-1", timestamp + 1000, 2);
        assertTrue(tracker.isBrokerAlive("broker-1"));
    }
}