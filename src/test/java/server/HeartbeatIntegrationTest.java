package server;

import grpc.HeartbeatRequest;
import grpc.HeartbeatResponse;
import grpc.HeartbeatServiceGrpc;
import grpc.ControllerDirective;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import server.config.BrokerConfig;
import server.internal.HeartbeatSender;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class HeartbeatIntegrationTest {

    private Server mockServer;
    private MockHeartbeatService mockService;
    private HeartbeatSender heartbeatSender;
    private BrokerConfig config;
    private static final int TEST_PORT = 50999;

    /**
     * Mock HeartbeatService that counts received heartbeats
     */
    private static class MockHeartbeatService extends HeartbeatServiceGrpc.HeartbeatServiceImplBase {
        private final AtomicInteger heartbeatCount = new AtomicInteger(0);
        private CountDownLatch firstHeartbeatLatch = new CountDownLatch(1);
        private volatile HeartbeatRequest lastRequest;

        @Override
        public void sendHeartbeat(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
            heartbeatCount.incrementAndGet();
            lastRequest = request;
            firstHeartbeatLatch.countDown();

            // Send a successful response
            HeartbeatResponse response = HeartbeatResponse.newBuilder()
                    .setAccepted(true)
                    .setControllerTimestamp(System.currentTimeMillis())
                    .setDirective(ControllerDirective.newBuilder()
                            .setType(ControllerDirective.DirectiveType.NONE)
                            .build())
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        public int getHeartbeatCount() {
            return heartbeatCount.get();
        }

        public HeartbeatRequest getLastRequest() {
            return lastRequest;
        }

        public boolean waitForFirstHeartbeat(long timeout, TimeUnit unit) throws InterruptedException {
            return firstHeartbeatLatch.await(timeout, unit);
        }

        public void reset() {
            heartbeatCount.set(0);
            // Reset the latch for the next wait
            firstHeartbeatLatch = new CountDownLatch(1);
        }
    }

    @BeforeEach
    public void setUp() throws IOException {
        // Create mock gRPC server
        mockService = new MockHeartbeatService();
        mockServer = ServerBuilder.forPort(TEST_PORT)
                .addService(mockService)
                .build()
                .start();

        // Create config with short heartbeat interval for testing
        // BrokerConfig(heartbeatIntervalMs, missedHeartbeatThreshold,
        // heartbeatTimeoutMs, heartbeatEnabled)
        config = new BrokerConfig(500L, 3, 5000L, true); // 500ms interval for quick testing

        // Create heartbeat sender
        heartbeatSender = new HeartbeatSender("test-broker-1", "localhost", 9090, config);
    }

    @AfterEach
    public void tearDown() {
        if (heartbeatSender != null) {
            heartbeatSender.stop();
        }
        if (mockServer != null) {
            mockServer.shutdown();
            try {
                mockServer.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                mockServer.shutdownNow();
            }
        }
    }

    @Test
    public void testHeartbeatSenderActuallyRuns() throws InterruptedException {
        // Start the heartbeat sender with mock server endpoint
        String controllerEndpoint = "localhost:" + TEST_PORT;
        heartbeatSender.start(controllerEndpoint);

        // Verify it's running
        assertTrue(heartbeatSender.isRunning());

        // Wait for first heartbeat to be received
        boolean received = mockService.waitForFirstHeartbeat(3, TimeUnit.SECONDS);
        assertTrue(received, "Should receive first heartbeat within 3 seconds");

        // Verify heartbeat was received
        assertTrue(mockService.getHeartbeatCount() > 0);
        assertNotNull(mockService.getLastRequest());

        // Verify request contains correct broker info
        HeartbeatRequest lastRequest = mockService.getLastRequest();
        assertEquals(1, lastRequest.getBrokerId()); // "test-broker-1" gets parsed as 1
        assertEquals("localhost", lastRequest.getHost());
        assertEquals(9090, lastRequest.getPort());
    }

    @Test
    public void testMultipleHeartbeats() throws InterruptedException {
        // Start the heartbeat sender
        heartbeatSender.start("localhost:" + TEST_PORT);

        // Wait for multiple heartbeats (with 500ms interval, expect at least 3 in 2
        // seconds)
        Thread.sleep(2000);

        // Stop the sender
        heartbeatSender.stop();
        assertFalse(heartbeatSender.isRunning());

        // Verify multiple heartbeats were sent
        int count = mockService.getHeartbeatCount();
        assertTrue(count >= 3, "Expected at least 3 heartbeats in 2 seconds, got: " + count);
    }

    @Test
    public void testHeartbeatStopsWhenStopped() throws InterruptedException {
        // Start the heartbeat sender
        heartbeatSender.start("localhost:" + TEST_PORT);

        // Wait for first heartbeat
        mockService.waitForFirstHeartbeat(3, TimeUnit.SECONDS);
        int initialCount = mockService.getHeartbeatCount();
        assertTrue(initialCount > 0);

        // Stop the heartbeat sender
        heartbeatSender.stop();
        assertFalse(heartbeatSender.isRunning());

        // Reset counter and wait a bit
        Thread.sleep(1000);
        int countAfterStop = mockService.getHeartbeatCount();

        // Verify no new heartbeats after stopping
        Thread.sleep(1000);
        assertEquals(countAfterStop, mockService.getHeartbeatCount(),
                "No new heartbeats should be sent after stopping");
    }

    @Test
    public void testRestartHeartbeatSender() throws InterruptedException {
        // Start the heartbeat sender
        heartbeatSender.start("localhost:" + TEST_PORT);
        mockService.waitForFirstHeartbeat(3, TimeUnit.SECONDS);
        assertTrue(mockService.getHeartbeatCount() > 0);

        // Stop it
        heartbeatSender.stop();
        mockService.reset();

        // Start it again
        heartbeatSender.start("localhost:" + TEST_PORT);

        // Verify it sends heartbeats again
        boolean received = mockService.waitForFirstHeartbeat(3, TimeUnit.SECONDS);
        assertTrue(received, "Should receive heartbeat after restart");
        assertTrue(mockService.getHeartbeatCount() > 0);
    }

    @Test
    public void testSequenceNumberIncrementsAcrossHeartbeats() throws InterruptedException {
        heartbeatSender.start("localhost:" + TEST_PORT);

        // Collect sequence numbers from multiple heartbeats
        long firstSeqNum = -1;
        long secondSeqNum = -1;

        // Wait for first heartbeat
        mockService.waitForFirstHeartbeat(3, TimeUnit.SECONDS);
        firstSeqNum = mockService.getLastRequest().getSequenceNumber();

        // Wait for another heartbeat
        Thread.sleep(600); // Wait for next heartbeat
        secondSeqNum = mockService.getLastRequest().getSequenceNumber();

        // Verify sequence numbers increment
        assertTrue(secondSeqNum > firstSeqNum,
                "Sequence numbers should increment: " + firstSeqNum + " -> " + secondSeqNum);
    }
}