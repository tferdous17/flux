package producer;

public interface Producer<K, V> {
    boolean send(ProducerRecord<K,V> record);
    void close();
}
