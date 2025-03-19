package producer;

import java.io.IOException;

public interface Producer<K, V> {
    void send(ProducerRecord<K,V> record) throws IOException;
    void sendDirect(ProducerRecord<K,V> record) throws IOException;
    void close();
}
