package metadata;

import broker.Partition;
import commons.FluxTopic;
import commons.IntRange;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * NOTE: Apache Kafka itself uses a log-based approach to storing metadata, where its essentially its own topic
 * with partitions, offsets, etc. It additionally comes with its own methods where you must make
 * network requests to a broker to retrieve metadata.
 *
 * However, for simplicity reasons, we are (currently) implementing an in-memory approach to storing Topic metadata
 * using the Repository pattern. Down the line we will refactor to more closely align with Kafka's implementation.
 */
public class InMemoryTopicMetadataRepository implements TopicMetadataRepository {
    private ConcurrentMap<String, FluxTopic> topicMetadata = new ConcurrentHashMap<>();

    @Override
    public void addNewTopic(String topicName, FluxTopic topic) {
        topicMetadata.put(topicName, topic);
    }

    @Override
    public boolean deleteTopic(String topicName) {
        if (topicMetadata.containsKey(topicName)) {
            topicMetadata.remove(topicName);
            return true;
        }
        return false;
    }

    @Override
    public boolean topicExists(String topicName) {
        return topicMetadata.containsKey(topicName);
    }

    @Override
    public Set<String> getActiveTopics() {
        return topicMetadata.keySet();
    }

    @Override
    public List<Partition> getPartitionsFor(String topicName) {
        if (!topicMetadata.containsKey(topicName)) {
            throw new IllegalArgumentException("Topic " + topicName + " does not exist. Create it first or check for typos.");
        }
        return topicMetadata.get(topicName).getPartitions();
    }

    @Override
    public IntRange getPartitionIdRangeForTopic(String topicName) {
        // Because we're creating and adding Partitions sequentially in Broker#createTopics, we can assume that
        // the first Partition in the list has the minimum ID
        if (!topicMetadata.containsKey(topicName)) {
            throw new IllegalArgumentException("Topic " + topicName + " does not exist. Create it first or check for typos.");
        }
        int startPartitionId = topicMetadata.get(topicName).getPartitions().get(0).getPartitionId();
        int endPartitionId = topicMetadata.get(topicName).getPartitions().get(topicMetadata.size() - 1).getPartitionId();
        return new IntRange(startPartitionId, endPartitionId);
    }
}
