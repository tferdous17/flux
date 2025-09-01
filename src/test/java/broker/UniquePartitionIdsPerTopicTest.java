package broker;

import commons.FluxTopic;
import metadata.InMemoryTopicMetadataRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import server.internal.Broker;
import server.internal.storage.Partition;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test to verify that partition IDs are unique per topic.
 * Each topic should have its own partition ID space starting from 0.
 */
public class UniquePartitionIdsPerTopicTest {
    
    private Broker broker;
    private InMemoryTopicMetadataRepository metadataRepo;
    
    @BeforeEach
    public void setUp() throws IOException {
        broker = new Broker("test-broker", "localhost", 50051, 2); // 2 default partitions
        metadataRepo = InMemoryTopicMetadataRepository.getInstance();
        // Reset the singleton instance
        metadataRepo = InMemoryTopicMetadataRepository.reset();
    }
    
    @Test
    public void testPartitionIdsStartFromZeroForEachTopic() throws IOException {
        // Create first topic with 3 partitions
        proto.Topic topic1 = proto.Topic.newBuilder()
                .setTopicName("topic-1")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
        
        broker.createTopics(List.of(topic1));
        
        // Create second topic with 5 partitions
        proto.Topic topic2 = proto.Topic.newBuilder()
                .setTopicName("topic-2")
                .setNumPartitions(5)
                .setReplicationFactor(1)
                .build();
        
        broker.createTopics(List.of(topic2));
        
        // Verify topic-1 partitions have IDs 0, 1, 2
        assertTrue(metadataRepo.topicExists("topic-1"));
        List<Partition> topic1Partitions = metadataRepo.getPartitionsFor("topic-1");
        assertNotNull(topic1Partitions);
        assertEquals(3, topic1Partitions.size());
        
        for (int i = 0; i < 3; i++) {
            assertEquals(i, topic1Partitions.get(i).getPartitionId(),
                    "topic-1 partition " + i + " should have ID " + i);
        }
        
        // Verify topic-2 partitions have IDs 0, 1, 2, 3, 4
        assertTrue(metadataRepo.topicExists("topic-2"));
        List<Partition> topic2Partitions = metadataRepo.getPartitionsFor("topic-2");
        assertNotNull(topic2Partitions);
        assertEquals(5, topic2Partitions.size());
        
        for (int i = 0; i < 5; i++) {
            assertEquals(i, topic2Partitions.get(i).getPartitionId(),
                    "topic-2 partition " + i + " should have ID " + i);
        }
        
        // Verify that partition IDs are NOT contiguous across topics
        // topic-1 has IDs 0-2, topic-2 also has IDs 0-4 (not 3-7)
        assertNotEquals(3, topic2Partitions.get(0).getPartitionId(),
                "topic-2's first partition should NOT start at ID 3");
    }
    
    @Test
    public void testPartitionAccessByTopicAndId() throws IOException {
        // Create a topic
        proto.Topic topic = proto.Topic.newBuilder()
                .setTopicName("test-topic")
                .setNumPartitions(4)
                .setReplicationFactor(1)
                .build();
        
        broker.createTopics(List.of(topic));
        
        // Test accessing partitions by topic and ID
        for (int i = 0; i < 4; i++) {
            Partition partition = broker.getPartition("test-topic", i);
            assertNotNull(partition);
            assertEquals(i, partition.getPartitionId());
        }
        
        // Test that accessing invalid partition ID throws exception
        assertThrows(IllegalArgumentException.class, () -> {
            broker.getPartition("test-topic", 4); // Out of range
        });
        
        assertThrows(IllegalArgumentException.class, () -> {
            broker.getPartition("test-topic", -1); // Negative ID
        });
    }
    
    @Test
    public void testPartitionIdRangesPerTopic() throws IOException {
        // Create multiple topics
        proto.Topic topic1 = proto.Topic.newBuilder()
                .setTopicName("range-test-1")
                .setNumPartitions(3)
                .setReplicationFactor(1)
                .build();
        
        proto.Topic topic2 = proto.Topic.newBuilder()
                .setTopicName("range-test-2")
                .setNumPartitions(7)
                .setReplicationFactor(1)
                .build();
        
        broker.createTopics(List.of(topic1));
        broker.createTopics(List.of(topic2));
        
        // Verify partition ID ranges
        var range1 = metadataRepo.getPartitionIdRangeForTopic("range-test-1");
        assertEquals(0, range1.start(), "range-test-1 should start at ID 0");
        assertEquals(2, range1.end(), "range-test-1 should end at ID 2");
        
        var range2 = metadataRepo.getPartitionIdRangeForTopic("range-test-2");
        assertEquals(0, range2.start(), "range-test-2 should start at ID 0");
        assertEquals(6, range2.end(), "range-test-2 should end at ID 6");
    }
}