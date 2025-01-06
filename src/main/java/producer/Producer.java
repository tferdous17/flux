package producer;

public interface Producer<K, V> {
    void send(ProducerRecord<K,V> record);
    void close();
}
