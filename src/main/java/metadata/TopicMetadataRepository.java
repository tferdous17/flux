package metadata;

import broker.Partition;
import commons.FluxTopic;
import commons.IntRange;

import java.util.List;
import java.util.Set;

public interface TopicMetadataRepository {
    void addNewTopic(String topicName, FluxTopic topic);
    boolean deleteTopic(String topicName);
    Set<String> getActiveTopics();
    List<Partition> getPartitions(String topicName);
    IntRange getPartitionIdRangeForTopic(String topicName);
}
