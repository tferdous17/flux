package producer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public interface Producer<K, V> {
    void send(ProducerRecord<K,V> record) throws IOException;
    void close();
}
