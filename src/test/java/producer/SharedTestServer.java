package producer;

import grpc.BrokerServer;
import proto.Topic;
import server.internal.Broker;

import java.io.IOException;
import java.util.List;

/**
 * Shared test server setup for producer tests.
 * This class manages a single gRPC server instance for all tests
 * to avoid port conflicts and improve test performance.
 */
public class SharedTestServer {
    private static BrokerServer server;
    private static Thread serverThread;
    private static Broker broker;
    private static boolean initialized = false;
    
    public static synchronized void startServer() throws IOException {
        if (initialized) {
            return; // Server already started
        }
        
        // Create broker and set it as active controller for testing
        broker = new Broker();
        broker.setIsActiveController(true);
        broker.initControllerBrokerMetadata();  // Initialize controller metadata
        
        // Create all test topics needed by various tests
        Topic bobTopic = Topic.newBuilder()
                .setTopicName("Bob")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
                
        Topic testTopic = Topic.newBuilder()
                .setTopicName("TestTopic")
                .setNumPartitions(5)
                .setReplicationFactor(1)
                .build();
                
        Topic topicTopic = Topic.newBuilder()
                .setTopicName("Topic")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
                
        Topic testTopicLowercase = Topic.newBuilder()
                .setTopicName("test-topic")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
        
        broker.createTopics(List.of(bobTopic, testTopic, topicTopic, testTopicLowercase));
        
        // Verify topics were created
        System.out.println("Created topics: Bob, TestTopic, Topic, test-topic");
        System.out.println("Active topics in repository: " + 
            metadata.InMemoryTopicMetadataRepository.getInstance().getActiveTopics());
        
        // Start gRPC server for Metadata service
        server = new BrokerServer(broker);
        serverThread = new Thread(() -> {
            try {
                server.start(50051);
                server.blockUntilShutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        serverThread.start();
        
        // Wait for server to start
        try {
            Thread.sleep(2000);  // Increased wait time to ensure server is fully ready
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        initialized = true;
    }
    
    public static synchronized void stopServer() {
        if (server != null) {
            try {
                server.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            server = null;
            serverThread = null;
            broker = null;
            initialized = false;
        }
    }
}