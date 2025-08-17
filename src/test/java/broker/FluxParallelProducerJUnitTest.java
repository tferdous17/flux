package broker;

/**
 * Test class demonstrating parallel producer functionality.
 * 
 * This test validates that multiple FluxProducers can connect to a single broker
 * and send messages concurrently without message loss.
 * - Multiple producers (5) sending messages simultaneously to one broker
 * - All messages arrive at the broker (verified by counting total messages)
 */
import grpc.BrokerServer;
import commons.FluxTopic;
import metadata.InMemoryTopicMetadataRepository;
import producer.FluxProducer;
import producer.ProducerRecord;
import org.junit.jupiter.api.*;
import org.tinylog.Logger;
import server.internal.Broker;
import server.internal.storage.Partition;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

public class FluxParallelProducerJUnitTest {
    
    private static final int TEST_PORT = 50051;
    private static final String TEST_TOPIC = "test-topic";
    private static final int NUM_PARTITIONS = 4;
    private static final String BROKER_ID = "TEST-BROKER";
    
    private Broker broker;
    private BrokerServer server;
    private Thread serverThread;
    
    @BeforeEach
    void setUp() throws Exception {
        // Clean up data directory
        cleanDataDirectory();
        
        // Create and start broker
        broker = new Broker(BROKER_ID, "localhost", TEST_PORT, NUM_PARTITIONS);
        server = new BrokerServer(broker);
        
        // Start server in separate thread
        serverThread = new Thread(() -> {
            try {
                server.start(TEST_PORT);
                server.blockUntilShutdown();
            } catch (Exception e) {
                Logger.error("Server error: ", e);
            }
        });
        serverThread.start();
        
        // Wait for server to start
        Thread.sleep(2000);
        
        // Create topic with partitions
        List<Partition> partitions = new ArrayList<>();
        for (int i = 1; i <= NUM_PARTITIONS; i++) {
            partitions.add(broker.getPartition(i));
        }
        FluxTopic testTopic = new FluxTopic(TEST_TOPIC, partitions, 1);
        InMemoryTopicMetadataRepository.getInstance().addNewTopic(TEST_TOPIC, testTopic);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        // Cleanup
        Thread.sleep(1000);
        server.stop();
        serverThread.interrupt();
        cleanDataDirectory();
    }
    
    @Test
    void testParallelProducersSendingToBroker() throws Exception {
        int numProducers = 5;
        int messagesPerProducer = 10;
        
        ExecutorService executor = Executors.newFixedThreadPool(numProducers);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(numProducers);
        
        List<FluxProducer<String, String>> producers = new ArrayList<>();
        
        // Create producers
        for (int i = 0; i < numProducers; i++) {
            FluxProducer<String, String> producer = new FluxProducer<>(new Properties(), 1, 2);
            producers.add(producer);
        }
        
        // Submit producer tasks to send messages in parallel
        for (int i = 0; i < numProducers; i++) {
            final int producerId = i;
            final FluxProducer<String, String> producer = producers.get(i);
            
            executor.submit(() -> {
                try {
                    startLatch.await(); // Wait for all producers to start together
                    
                    Logger.info("Producer {} starting to send messages", producerId);
                    
                    // Send messages
                    for (int j = 0; j < messagesPerProducer; j++) {
                        ProducerRecord<String, String> record = new ProducerRecord<>(
                            TEST_TOPIC,
                            "key-" + j,
                            "Message from producer " + producerId + " #" + j
                        );
                        
                        producer.send(record);
                    }
                    
                    producer.forceFlush();
                    Logger.info("Producer {} finished sending messages", producerId);
                    
                    completeLatch.countDown();
                    
                } catch (Exception e) {
                    Logger.error("Producer {} failed: ", producerId, e);
                    completeLatch.countDown();
                }
            });
        }
        
        // Start all producers simultaneously
        Logger.info("Starting {} producers to send {} messages each in parallel", numProducers, messagesPerProducer);
        startLatch.countDown();
        
        // Wait for all producers to complete
        boolean completed = completeLatch.await(30, TimeUnit.SECONDS);
        assertTrue(completed, "All producers should complete within timeout");
        
        executor.shutdown();
        
        // Wait for messages to be processed
        Thread.sleep(2000);
        
        // Verify messages were received by the broker
        int totalMessages = 0;
        for (int i = 1; i <= NUM_PARTITIONS; i++) {
            int count = broker.getPartition(i).getCurrentOffset();
            totalMessages += count;
            Logger.info("Partition {} received {} messages", i, count);
        }
        
        Logger.info("Total messages received by broker: {}", totalMessages);
        
        // Verify all messages were received
        assertEquals(numProducers * messagesPerProducer, totalMessages, 
            "Broker should receive all messages sent by parallel producers");
        
        // Close all producers
        for (FluxProducer<String, String> producer : producers) {
            producer.close();
        }
    }
    
    private void cleanDataDirectory() {
        File dataDir = new File("data");
        if (dataDir.exists() && dataDir.isDirectory()) {
            File[] files = dataDir.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.getName().endsWith(".log") || file.getName().endsWith(".index")) {
                        file.delete();
                    }
                }
            }
        }
    }
}