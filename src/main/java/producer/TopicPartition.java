package producer;

import java.util.Objects;

/**
 * A simple compound key representing a topic and partition pair.
 * Used as a key in RecordAccumulator to support multi-topic batching.
 */
public class TopicPartition {
    private final String topic;
    private final int partition;

    public TopicPartition(String topic, int partition) {
        this.topic = topic;
        this.partition = partition;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TopicPartition that = (TopicPartition) o;
        return partition == that.partition && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}