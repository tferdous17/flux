package consumer;

import commons.header.Header;
import commons.headers.Headers;

import java.util.Optional;

public class ConsumerRecord<K, V> {
    private String topic;
    private Optional<Integer> partition;
    private long offset;
    private long timestamp;
    private Optional<K> key = Optional.empty();
    private V value;
    private Headers headers;

    public ConsumerRecord(String topic, Integer partition, long offset, long timestamp) {
        this.topic = topic;
        this.partition = Optional.of(partition);
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public ConsumerRecord(String topic, Integer partition, long offset, long timestamp, K key, V value, Headers headers) {
        this.topic = topic;
        this.partition = Optional.of(partition);;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = Optional.of(key);
        this.value = value;
        this.headers = headers;
    }

    public Iterable<Header> getHeaders() {
        return this.headers;
    }

    public K getKey() {
        return key.orElse(null);
    }

    public long getOffset() {
        return this.offset;
    }

    public int getPartition() {
        return partition.orElse(-1);
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public String getTopic() {
        return this.topic;
    }

    public V getValue() {
        return this.value;
    }

    @Override
    public String toString() {
        StringBuilder stringBuffer = new StringBuilder();

        if (headers != null) {
            StringBuilder headersString = new StringBuilder();

            for (Header header : this.headers) {
                if (header != null) {
                    headersString.append(header.toString());
                }
            }

            stringBuffer.append("Headers: ").append(headersString).append("\n");
        }

        stringBuffer
                .append("Topic: ").append(String.valueOf(getTopic())).append("\n")
                .append("Timestamp: ").append(getTimestamp()).append("\n")
                .append("Offset: ").append(getOffset()).append("\n")
                .append("Partition: ").append(getPartition()).append("\n")
                .append("Key: ").append(getKey()).append("\n")
                .append("Value: ").append(getValue());
        return stringBuffer.toString();
    }

}
