/*
A key/value pair to be sent to Kafka.
This consists of a topic name to which the record is being sent, an optional partition number, and an optional key and value.

If a valid partition number is specified that partition will be used when sending the record.

If no partition is specified but a key is present a partition will be chosen using a hash of the key. If neither key nor partition is present a partition will be assigned in a round-robin fashion.

The record also has an associated timestamp. If the user did not provide a timestamp, the producer will stamp the record with its current time. The timestamp eventually used by Kafka depends on the timestamp type configured for the topic.
 */
package producer;
import commons.header.Header;
import commons.headers.Headers;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class ProducerRecord<K, V> {
    private String topic;
    private Long timestamp;
    private V value;
    private Optional<Integer> partitionNumber = Optional.empty();
    private Optional<K> key = Optional.empty();
    private Headers headers = new Headers();

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.value = value;
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
    }

    public ProducerRecord(String topic, Integer partition, Long timestamp, K key, V value, Iterable<Header> headers) {
        this.topic = topic;
        this.timestamp = timestamp;
        this.value = value;
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.headers = convertToHeaders(headers);
    }

    public ProducerRecord(String topic, Integer partition, K key, V value) {
        this.topic = topic;
        this.value = value;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
    }

    public ProducerRecord(String topic, Integer partition, K key, V value, Iterable<Header> headers) {
        this.topic = topic;
        this.value = value;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.partitionNumber = Optional.ofNullable(partition);
        this.key = Optional.ofNullable(key);
        this.headers = convertToHeaders(headers);
    }

    public ProducerRecord(String topic, K key, V value) {
        this.topic = topic;
        this.value = value;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        this.key = Optional.ofNullable(key);
        if (this.key.isPresent()) {
            this.partitionNumber = Optional.of(Math.abs(key.hashCode()));
        }
    }

    public ProducerRecord(String topic, V value) {
        this.topic = topic;
        this.value = value;
        this.timestamp = LocalDateTime.now()
                .atZone(ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
        // TODO: Round Robin Implementation for partition number below;
        this.partitionNumber = Optional.empty();
    }

    // METHODS
    private static Headers convertToHeaders(Iterable<Header> headers) {
        Headers convertedHeaders = new Headers();
        if (headers != null) {
            headers.forEach(convertedHeaders::add);
        }
        return convertedHeaders;
    }
    public String getTopic() {
        return topic;
    }

    public Headers getHeaders() {
        return headers;
    }

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
        return value;
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
                .append("Value: ").append(getValue()).append("\n")
                .append("Headers: ").append(Arrays.toString(getHeaders().toArray()));
        return stringBuffer.toString();
    }
}
