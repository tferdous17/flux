/*
A key/value pair to be sent to Kafka.
This consists of a topic name to which the record is being sent, an optional partition number, and an optional key and value.

If a valid partition number is specified that partition will be used when sending the record.

If no partition is specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is present a partition will be assigned in a round-robin fashion.

The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for the topic.


 */
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Optional;
public class ProducerRecord<K, V>{

    private String topic;
    private Long timestamp;
    private Optional<Integer> partitionNumber;
    private Optional<K> key;
    private Optional<V> value;

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);
    }

    // TODO No Header class.
    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);
        // INSERT HEADER

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

    // TODO No Header class.
    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this.topic = topic;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.value = Optional.ofNullable(value);
        // INSERT HEADER
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

    // INSERT METHODS BELOW.
}
