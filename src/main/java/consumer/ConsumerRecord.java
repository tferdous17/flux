package consumer;

import commons.header.Header;
import commons.headers.Headers;

import java.util.Optional;

public class ConsumerRecord<K, V> {
    private String topic;
    private int partition;
    private long offset;
    private long timestamp;
    private Optional<K> key = null;
    private V value;
    private Headers headers;

    public ConsumerRecord(String topic, int partition, long offset, long timestamp) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public ConsumerRecord(String topic, int partition, long offset, long timestamp, Optional<K> key, V value, Headers headers) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
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
        return this.partition;
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

            stringBuffer.append("Headers: ").append(headersString);
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
