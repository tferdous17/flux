package producer;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Future;

public interface Producer<K, V> {
    /**
     * Send a record asynchronously (backward compatibility)
     * @param record the record to send
     * @throws IOException if an error occurs
     */
    void send(ProducerRecord<K,V> record) throws IOException;
    
    /**
     * Send a record asynchronously with a callback
     * @param record the record to send
     * @param callback the callback to invoke when the record is acknowledged
     * @return a future that will be completed when the record is acknowledged
     * @throws IOException if an error occurs
     */
    Future<RecordMetadata> sendAsync(ProducerRecord<K,V> record, Callback callback) throws IOException;
    
    void close();
}
