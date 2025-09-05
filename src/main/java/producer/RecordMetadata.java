package producer;

import java.util.Objects;

/**
 * The metadata for a record that has been acknowledged by the server.
 * This class is immutable and provides information about where the record was stored.
 * 
 * This class matches Kafka's RecordMetadata for compatibility.
 */
public final class RecordMetadata {
    
    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final int serializedKeySize;
    private final int serializedValueSize;
    
    /**
     * Creates a new RecordMetadata instance.
     * 
     * @param topic The topic the record was sent to
     * @param partition The partition the record was sent to
     * @param offset The offset of the record in the partition
     * @param timestamp The timestamp of the record
     * @param serializedKeySize The size of the serialized key in bytes (-1 if key is null)
     * @param serializedValueSize The size of the serialized value in bytes
     */
    public RecordMetadata(String topic, int partition, long offset, long timestamp,
                         int serializedKeySize, int serializedValueSize) {
        this.topic = Objects.requireNonNull(topic, "topic cannot be null");
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
    }
    
    /**
     * Creates a RecordMetadata with unknown offset (used before server acknowledgment).
     * 
     * @param topic The topic the record was sent to
     * @param partition The partition the record was sent to
     * @param timestamp The timestamp of the record
     * @param serializedKeySize The size of the serialized key in bytes
     * @param serializedValueSize The size of the serialized value in bytes
     */
    public RecordMetadata(String topic, int partition, long timestamp,
                         int serializedKeySize, int serializedValueSize) {
        this(topic, partition, -1L, timestamp, serializedKeySize, serializedValueSize);
    }
    
    /**
     * @return The topic the record was sent to
     */
    public String topic() {
        return topic;
    }
    
    /**
     * @return The partition the record was sent to
     */
    public int partition() {
        return partition;
    }
    
    /**
     * @return The offset of the record in the partition (-1 if unknown)
     */
    public long offset() {
        return offset;
    }
    
    /**
     * @return The timestamp of the record
     */
    public long timestamp() {
        return timestamp;
    }
    
    /**
     * @return The size of the serialized key in bytes (-1 if key is null)
     */
    public int serializedKeySize() {
        return serializedKeySize;
    }
    
    /**
     * @return The size of the serialized value in bytes
     */
    public int serializedValueSize() {
        return serializedValueSize;
    }
    
    /**
     * @return Whether this metadata has a valid offset
     */
    public boolean hasOffset() {
        return offset >= 0;
    }
    
    @Override
    public String toString() {
        return String.format("RecordMetadata(topic=%s, partition=%d, offset=%d, timestamp=%d, " +
                           "serializedKeySize=%d, serializedValueSize=%d)",
                           topic, partition, offset, timestamp, serializedKeySize, serializedValueSize);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordMetadata that = (RecordMetadata) o;
        return partition == that.partition &&
               offset == that.offset &&
               timestamp == that.timestamp &&
               serializedKeySize == that.serializedKeySize &&
               serializedValueSize == that.serializedValueSize &&
               Objects.equals(topic, that.topic);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(topic, partition, offset, timestamp, serializedKeySize, serializedValueSize);
    }
}