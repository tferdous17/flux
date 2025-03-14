package producer;

import commons.header.Properties;

import java.io.IOException;
import java.util.Map;

public class FluxProducer<K, V> implements Producer {
    private Map<String, String> configs;
    RecordAccumulator recordAccumulator = new RecordAccumulator();

    public FluxProducer(Map<String, String> configs) {
        this.configs = configs;
    }

    public FluxProducer(Properties props) {
        this.configs = props.getAllProperties();
    }

    // Batching will be better for when Flux is distributed
    @Override
    public void send(ProducerRecord record) throws IOException {
        byte[] serializedData = ProducerRecordCodec.serialize(record, record.getKey().getClass(), record.getValue().getClass());
        recordAccumulator.append(serializedData);
    }

    // Send record directly to Broker w/o batching.
    public void sendDirect(ProducerRecord record) {
        byte[] serializedData = ProducerRecordCodec.serialize(record, record.getKey().getClass(), record.getValue().getClass());
    }

    @Override
    public void close() {
        //TODO: RecordAccumulator should flush any remaining records
    }
}
