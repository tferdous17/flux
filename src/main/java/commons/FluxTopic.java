package commons;

import broker.Partition;

import java.util.List;

public class FluxTopic {
    private String topicName;
    private List<Partition> partitions;
    private int replicationFactor;

    public FluxTopic(String topicName, List<Partition> partitions, int replicationFactor) {
        this.topicName = topicName;
        this.partitions = partitions;
        this.replicationFactor = replicationFactor;
    }

    public String getTopicName() {
        return topicName;
    }

    public List<Partition> getPartitions() {
        return partitions;
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }
}
