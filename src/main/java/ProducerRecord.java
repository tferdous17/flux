/*
A key/value pair to be sent to Kafka.
This consists of a topic name to which the record is being sent, an optional partition number, and an optional key and value.

If a valid partition number is specified that partition will be used when sending the record.

If no partition is specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is present a partition will be assigned in a round-robin fashion.

The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for the topic.


 */
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class ProducerRecord<K, V> {
    private String topic;
    private Long timestamp;
    private Optional<Integer> partitionNumber = Optional.empty();
    private Optional<K> key = Optional.empty();
    private Optional<V> value = Optional.empty();
     private Iterable<Header> headers;

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);
    }

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);

        if (headers != null) {
            List<Header> headerList = new ArrayList<>();
            headers.forEach(headerList::add);
            this.headers = Collections.unmodifiableList(headerList);
        } else {
            this.headers = Collections.emptyList();
        }

    }

    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this.topic = topic;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);
    }

    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this.topic = topic;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);
        if (headers != null) {
            List<Header> headerList = new ArrayList<>();
            headers.forEach(headerList::add);
            this.headers = Collections.unmodifiableList(headerList);
        } else {
            this.headers = Collections.emptyList();
        }
    }

    public ProducerRecord(String topic, K key, V value) {
        this.topic = topic;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);
    }

    public ProducerRecord(String topic, V value) {
        this.topic = topic;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.value = Optional.ofNullable(value);
    }

    // METHODS
    public String getTopic() {
        return topic;
    }

    //TODO: Add Headers class.

    public K getKey() {
        return key.orElse(null);
    }

    public Integer getPartitionNumber() {
        return partitionNumber.orElse(null);
    }

    public Long getTimestamp() {
        return timestamp;
    }


    public V getValue() {
        return value.orElse(null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProducerRecord<?, ?> that = (ProducerRecord<?, ?>) o;
        return Objects.equals(topic, that.topic) &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(partitionNumber, that.partitionNumber) &&
                Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(topic, timestamp, partitionNumber, key, value);
    }

    @Override
    public String toString() {
        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer
                .append("Topic: ").append(String.valueOf(getTopic())).append("\n")
                .append("Timestamp: ").append(getTimestamp()).append("\n")
                .append("PartitionNumber: ").append(getPartitionNumber()).append("\n")
                .append("Key: ").append(getKey()).append("\n")
                .append("Value: ").append(getValue());
        return stringBuffer.toString();
    }
}
